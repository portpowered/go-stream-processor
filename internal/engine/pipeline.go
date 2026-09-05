package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineState represents the current lifecycle state of a Pipeline.
type PipelineState int32

const (
	// PipelineInitializing is the state before Start() is called.
	PipelineInitializing PipelineState = iota
	// PipelineRunning is the state after Start() has launched all goroutines.
	PipelineRunning
	// PipelinePaused is the state after Pause() — nodes are alive but not processing.
	PipelinePaused
	// PipelineStopping is the state after Stop() — a then_stop barrier is propagating.
	PipelineStopping
	// PipelineStopped is the terminal state after all nodes have exited.
	PipelineStopped
	// PipelineTerminated is the terminal state after Terminate() — nodes exited immediately.
	PipelineTerminated
	// PipelineError is the terminal state when the pipeline encountered an error.
	PipelineError
)

// String returns a human-readable name for the pipeline state.
func (s PipelineState) String() string {
	switch s {
	case PipelineInitializing:
		return "Initializing"
	case PipelineRunning:
		return "Running"
	case PipelinePaused:
		return "Paused"
	case PipelineStopping:
		return "Stopping"
	case PipelineStopped:
		return "Stopped"
	case PipelineTerminated:
		return "Terminated"
	case PipelineError:
		return "Error"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// Pipeline is an executable stream processing graph. It manages the lifecycle
// of all node goroutines, buffers, and the checkpoint controller.
type Pipeline struct {
	name string

	runtimes       []*NodeRuntime
	sourceRuntimes []*NodeRuntime
	sourceNodes    []*SourceNode
	sourceOutputs  map[string][]*Buffer
	controller     *Controller
	buffers        []*Buffer

	// state is accessed atomically for thread-safe reads.
	state atomic.Int32

	// stopEpoch tracks the next checkpoint epoch for Stop().
	stopEpoch uint64
	epochMu   sync.Mutex

	// error tracking
	errCounts *errorCounts
	dlBuffer  *Buffer
	dlRuntime *NodeRuntime

	cancel context.CancelFunc
	done   chan struct{}
}

// Start launches all node goroutines and begins processing. The provided
// context controls the pipeline's lifetime; cancelling it stops all nodes.
func (p *Pipeline) Start(ctx context.Context) {
	if !p.compareAndSwapState(PipelineInitializing, PipelineRunning) {
		return
	}

	pipeCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.done = make(chan struct{})

	// Start the checkpoint controller.
	p.controller.Start(pipeCtx)

	// Start all regular node runtimes.
	for _, rt := range p.runtimes {
		rt.Start(pipeCtx)
	}

	// Start the dead-letter runtime separately so it is not included in the
	// regular runtimes WaitGroup. Its lifecycle is managed independently —
	// the monitor will signal it to stop after all regular runtimes exit.
	if p.dlRuntime != nil {
		p.dlRuntime.Start(pipeCtx)
	}

	// Start source goroutines. When a source finishes producing data,
	// inject a stop signal into its output buffers so downstream nodes
	// know to drain and shut down.
	var sourceWg sync.WaitGroup
	for i, src := range p.sourceNodes {
		sourceWg.Add(1)
		go func(s *SourceNode, rt *NodeRuntime, outputs []*Buffer) {
			defer sourceWg.Done()
			_ = s.RunSource(pipeCtx)

			// Source finished — propagate stop signal downstream.
			stopMsg := NewSignalMessage(SignalMessage{SignalType: SignalTypeStop})
			for _, buf := range outputs {
				// Use background context so stop signals are not blocked
				// by pipeline context cancellation.
				_ = buf.Send(context.Background(), stopMsg)
			}

			// Stop the source's runtime (it has no inputs to close naturally).
			rt.Stop()
		}(src, p.sourceRuntimes[i], p.sourceOutputs[src.ID()])
	}

	// Monitor: when all runtimes exit, close done channel and set terminal state.
	// If any runtime exits with a non-context error, cancel the pipeline
	// context to unblock all other nodes (important for FailPipeline strategy).
	go func() {
		errs := make(chan error, len(p.runtimes))
		var wg sync.WaitGroup
		for _, rt := range p.runtimes {
			wg.Add(1)
			go func(r *NodeRuntime) {
				defer wg.Done()
				errs <- r.Wait()
			}(rt)
		}

		// Detect the first node error and cancel the pipeline.
		go func() {
			for e := range errs {
				if e != nil && e != context.Canceled {
					p.storeState(PipelineError)
					if p.cancel != nil {
						p.cancel()
					}
				}
			}
		}()

		wg.Wait()
		close(errs)
		p.controller.Stop()

		// All regular runtimes have exited. If a dead-letter runtime exists,
		// signal it to stop (so it drains remaining messages and exits), then
		// wait for it with a timeout to avoid hanging forever.
		if p.dlRuntime != nil && p.dlBuffer != nil {
			stopMsg := NewSignalMessage(SignalMessage{SignalType: SignalTypeStop})
			_ = p.dlBuffer.Send(context.Background(), stopMsg)

			dlDone := make(chan struct{})
			go func() {
				_ = p.dlRuntime.Wait()
				close(dlDone)
			}()

			select {
			case <-dlDone:
				// Dead-letter runtime exited cleanly.
			case <-time.After(10 * time.Second):
				// Timeout — force-stop the dead-letter runtime.
				p.dlRuntime.Stop()
			}
		}

		// Transition to terminal state.
		state := p.State()
		if state != PipelineError && state != PipelineTerminated {
			p.storeState(PipelineStopped)
		}
		close(p.done)
	}()
}

// Stop triggers graceful shutdown by injecting a then_stop checkpoint barrier
// through all source nodes. Nodes will finish processing in-flight data,
// complete the final checkpoint, and then exit.
func (p *Pipeline) Stop(ctx context.Context) error {
	state := p.State()
	if state != PipelineRunning && state != PipelinePaused {
		return fmt.Errorf("cannot stop pipeline in state %s", state)
	}

	p.compareAndSwapState(state, PipelineStopping)

	// Allocate an epoch for the stop checkpoint.
	p.epochMu.Lock()
	p.stopEpoch++
	epoch := p.stopEpoch
	p.epochMu.Unlock()

	// Initiate a checkpoint with thenStop=true, which causes all nodes
	// to complete the checkpoint and then shut down.
	return p.controller.InitiateCheckpoint(ctx, epoch, true, p.sourceRuntimes)
}

// Pause sends a pause control message to all nodes. Nodes stop pulling from
// input buffers but remain alive. The pipeline transitions to Paused state.
func (p *Pipeline) Pause(ctx context.Context) error {
	if !p.compareAndSwapState(PipelineRunning, PipelinePaused) {
		return fmt.Errorf("cannot pause pipeline in state %s", p.State())
	}

	ctrl := NewPauseControl()
	for _, rt := range p.runtimes {
		if err := rt.SendControl(ctx, ctrl); err != nil {
			return err
		}
	}
	return nil
}

// Resume sends a resume control message to all nodes. Nodes resume processing
// from where they left off. The pipeline transitions back to Running state.
func (p *Pipeline) Resume(ctx context.Context) error {
	if !p.compareAndSwapState(PipelinePaused, PipelineRunning) {
		return fmt.Errorf("cannot resume pipeline in state %s", p.State())
	}

	ctrl := NewResumeControl()
	for _, rt := range p.runtimes {
		if err := rt.SendControl(ctx, ctrl); err != nil {
			return err
		}
	}
	return nil
}

// Terminate sends a terminate control message to all nodes. Nodes exit
// immediately without completing a final checkpoint. The pipeline transitions
// to Terminated state. If the pipeline does not exit within the provided
// context's deadline, the context is cancelled to force shutdown.
func (p *Pipeline) Terminate(ctx context.Context) error {
	state := p.State()
	if state != PipelineRunning && state != PipelinePaused {
		return fmt.Errorf("cannot terminate pipeline in state %s", state)
	}

	p.storeState(PipelineTerminated)

	ctrl := NewTerminateControl()
	for _, rt := range p.runtimes {
		// Best-effort send — if context expires, cancel will clean up.
		_ = rt.SendControl(ctx, ctrl)
	}

	// Also terminate the dead-letter runtime so it exits immediately
	// alongside regular runtimes rather than waiting for the monitor's
	// stop signal.
	if p.dlRuntime != nil {
		_ = p.dlRuntime.SendControl(ctx, ctrl)
	}

	// Cancel the pipeline context to ensure all goroutines exit.
	if p.cancel != nil {
		p.cancel()
	}

	return nil
}

// Wait blocks until all nodes have exited.
func (p *Pipeline) Wait() error {
	if p.done == nil {
		return nil
	}
	<-p.done
	return nil
}

// State returns the current lifecycle state of the pipeline. It is safe to
// call from any goroutine.
func (p *Pipeline) State() PipelineState {
	return PipelineState(p.state.Load())
}

// Name returns the pipeline's name.
func (p *Pipeline) Name() string {
	return p.name
}

// ErrorCounts returns a snapshot of per-node error counts. The map keys are
// node IDs and values are the number of errors encountered by that node.
// Only nodes with at least one error are included.
func (p *Pipeline) ErrorCounts() map[string]int64 {
	if p.errCounts == nil {
		return nil
	}
	return p.errCounts.snapshot()
}

// storeState unconditionally sets the pipeline state.
func (p *Pipeline) storeState(s PipelineState) {
	p.state.Store(int32(s))
}

// compareAndSwapState atomically transitions from old to new state.
func (p *Pipeline) compareAndSwapState(old, new PipelineState) bool {
	return p.state.CompareAndSwap(int32(old), int32(new))
}
