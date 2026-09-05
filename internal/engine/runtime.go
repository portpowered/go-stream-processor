package engine

import (
	"context"
	"sync"
	"time"
)

// NodeRuntime manages a node's goroutine lifecycle and its main processing loop.
// It multiplexes input buffers, a control channel, an optional tick timer, and
// context cancellation into a single select loop. Data messages are dispatched
// to the node's ProcessMessage handler; signals (barriers, watermarks, stop)
// are handled by the runtime; control messages are handled out-of-band.
type NodeRuntime struct {
	node     Node
	nodeType NodeType

	inputs  []*Buffer
	outputs []*Buffer

	controlCh chan ControlMessage

	tickInterval time.Duration

	// merged receives messages from all input buffers, tagged with input index.
	merged chan indexedMessage

	// aligner handles barrier alignment for multi-input nodes.
	aligner *BarrierAligner

	// watermarks tracks per-input watermarks and computes the output watermark.
	watermarks *WatermarkHolder

	// activeInputs tracks how many inputs have not yet sent a stop signal.
	// For multi-input nodes, the node only forwards stop and shuts down
	// when all inputs have stopped.
	activeInputs int

	// paused gates whether data is processed.
	mu     sync.Mutex
	paused bool

	// completion reporting
	completionCh chan<- NodeCompletion
	stoppedCh    chan<- NodeStopped

	// error handling
	onError errorHandler

	// lifecycle
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// NodeRuntimeConfig configures a NodeRuntime.
type NodeRuntimeConfig struct {
	Node         Node
	NodeType     NodeType
	Inputs       []*Buffer
	Outputs      []*Buffer
	TickInterval time.Duration
	// ControlBufferSize is the capacity of the control channel. Defaults to 16.
	ControlBufferSize int
	// CompletionCh, if set, receives a NodeCompletion report each time this
	// node finishes processing a checkpoint barrier (after snapshotting state
	// and forwarding the barrier to outputs).
	CompletionCh chan<- NodeCompletion
	// StoppedCh, if set, receives a NodeStopped report when this node exits
	// its processing loop (due to stop signal, context cancellation, or
	// all inputs closing).
	StoppedCh chan<- NodeStopped
	// OnError is called when ProcessMessage returns an error. If nil,
	// errors propagate and stop the node (legacy behavior).
	OnError errorHandler
}

// NewNodeRuntime creates a new NodeRuntime for the given node.
func NewNodeRuntime(cfg NodeRuntimeConfig) *NodeRuntime {
	ctrlSize := cfg.ControlBufferSize
	if ctrlSize <= 0 {
		ctrlSize = 16
	}
	numInputs := len(cfg.Inputs)
	if numInputs == 0 {
		numInputs = 1 // sources use 1 so stop logic works uniformly
	}
	return &NodeRuntime{
		node:         cfg.Node,
		nodeType:     cfg.NodeType,
		inputs:       cfg.Inputs,
		outputs:      cfg.Outputs,
		controlCh:    make(chan ControlMessage, ctrlSize),
		tickInterval: cfg.TickInterval,
		activeInputs: numInputs,
		merged:       make(chan indexedMessage, DefaultBufferCapacity),
		aligner:      NewBarrierAligner(len(cfg.Inputs)),
		watermarks:   NewWatermarkHolder(len(cfg.Inputs)),
		completionCh: cfg.CompletionCh,
		stoppedCh:    cfg.StoppedCh,
		onError:      cfg.OnError,
		done:         make(chan struct{}),
	}
}

// Start launches the node's processing goroutine. It should be called once.
func (nr *NodeRuntime) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	nr.cancel = cancel

	// Launch merger goroutines to multiplex all input buffers into merged.
	// Each merger tags messages with its input index for barrier alignment.
	if len(nr.inputs) > 0 {
		var mergeWg sync.WaitGroup
		for i, buf := range nr.inputs {
			mergeWg.Add(1)
			go func(idx int, b *Buffer) {
				defer mergeWg.Done()
				for {
					msg, err := b.Recv(runCtx)
					if err != nil {
						return
					}
					select {
					case nr.merged <- indexedMessage{InputIndex: idx, Msg: msg}:
					case <-runCtx.Done():
						return
					}
				}
			}(i, buf)
		}

		// Close merged channel when all mergers are done.
		go func() {
			mergeWg.Wait()
			close(nr.merged)
		}()
	} else {
		// Source nodes have no inputs. Set merged to nil so the select
		// loop never reads from it; the source relies on control channel
		// and context cancellation.
		nr.merged = nil
	}

	// Launch the main processing loop.
	go nr.run(runCtx)
}

// Stop requests graceful shutdown. It cancels the runtime's context and waits
// for in-flight processing to complete.
func (nr *NodeRuntime) Stop() {
	if nr.cancel != nil {
		nr.cancel()
	}
}

// Wait blocks until the node's goroutine has exited.
func (nr *NodeRuntime) Wait() error {
	<-nr.done
	return nr.err
}

// SendControl sends a control message to the node's out-of-band control channel.
func (nr *NodeRuntime) SendControl(ctx context.Context, msg ControlMessage) error {
	select {
	case nr.controlCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// emitter routes emitted messages to all output buffers.
type runtimeEmitter struct {
	outputs []*Buffer
	ctx     context.Context
}

func (e *runtimeEmitter) Emit(ctx context.Context, msg Message) error {
	for _, out := range e.outputs {
		if err := out.Send(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// run is the main processing loop. It selects over merged inputs, control
// channel, optional tick timer, registered timers, and context cancellation.
func (nr *NodeRuntime) run(ctx context.Context) {
	defer close(nr.done)
	defer func() {
		if nr.stoppedCh != nil {
			nr.stoppedCh <- NodeStopped{NodeID: nr.node.ID()}
		}
	}()

	emit := &runtimeEmitter{outputs: nr.outputs, ctx: ctx}

	// Check if the node supports timers.
	var tmr *timerManager
	if provider, ok := nr.node.(timerManagerProvider); ok {
		tmr = provider.getTimerManager()
	}

	var ticker *time.Ticker
	var tickCh <-chan time.Time
	if nr.tickInterval > 0 {
		ticker = time.NewTicker(nr.tickInterval)
		tickCh = ticker.C
		defer ticker.Stop()
	}

	var nextTimer *time.Timer
	defer func() {
		if nextTimer != nil {
			nextTimer.Stop()
		}
	}()

	for {
		// Compute timer channel based on next pending timer.
		var timerCh <-chan time.Time
		if tmr != nil {
			if next, ok := tmr.Peek(); ok {
				d := time.Until(next.FireAt)
				if d <= 0 {
					// Timer already expired — fire immediately.
					if !nr.isPaused() {
						if err := nr.fireExpiredTimers(ctx, tmr, emit); err != nil {
							nr.err = err
							return
						}
					}
					continue
				}
				if nextTimer == nil {
					nextTimer = time.NewTimer(d)
				} else {
					if !nextTimer.Stop() {
						select {
						case <-nextTimer.C:
						default:
						}
					}
					nextTimer.Reset(d)
				}
				timerCh = nextTimer.C
			}
		}

		select {
		case <-ctx.Done():
			nr.err = ctx.Err()
			return

		case ctrl, ok := <-nr.controlCh:
			if !ok {
				return
			}
			nr.handleControl(ctx, ctrl, emit)
			// Check if terminate was requested.
			if ctrl.Type == ControlTypeTerminate {
				return
			}

		case im, ok := <-nr.merged:
			if !ok {
				// All inputs closed — drain complete.
				return
			}
			// If paused, wait for resume or cancellation.
			if nr.isPaused() {
				nr.waitForResume(ctx)
				if ctx.Err() != nil {
					nr.err = ctx.Err()
					return
				}
			}
			// Route through barrier aligner for multi-input alignment.
			if err := nr.handleIndexedMessage(ctx, im, emit); err != nil {
				nr.err = err
				return
			}

		case <-timerCh:
			if nr.isPaused() {
				continue
			}
			if err := nr.fireExpiredTimers(ctx, tmr, emit); err != nil {
				nr.err = err
				return
			}

		case <-tickCh:
			if nr.isPaused() {
				continue
			}
			// Tick handler — dispatch to node if it implements TickHandler.
			if th, ok := nr.node.(TickHandler); ok {
				if err := th.HandleTick(ctx, emit); err != nil {
					nr.err = err
					return
				}
			}
		}
	}
}

// fireExpiredTimers pops and handles all expired timers via the node's
// TimerHandler interface.
func (nr *NodeRuntime) fireExpiredTimers(ctx context.Context, tmr *timerManager, emit Emitter) error {
	expired := tmr.PopExpired(time.Now())
	if len(expired) == 0 {
		return nil
	}
	th, ok := nr.node.(TimerHandler)
	if !ok {
		return nil
	}
	for _, entry := range expired {
		if err := th.HandleTimer(ctx, entry.Name, entry.FireAt, emit); err != nil {
			return err
		}
	}
	return nil
}

// handleIndexedMessage routes an indexed message through the barrier aligner
// and then dispatches any resulting messages for processing.
func (nr *NodeRuntime) handleIndexedMessage(ctx context.Context, im indexedMessage, emit Emitter) error {
	// Intercept watermark signals to route through the WatermarkHolder
	// before the barrier aligner, since we need the input index.
	if im.Msg.IsSignal() && im.Msg.Signal != nil && im.Msg.Signal.SignalType == SignalTypeWatermark && im.Msg.Signal.Watermark != nil {
		return nr.handleIndexedWatermark(ctx, im.InputIndex, im.Msg.Signal.Watermark, emit)
	}

	toProcess, alignedBarrier := nr.aligner.OnMessage(im)

	// Process any messages released by the aligner (includes buffered data
	// from already-barriered inputs after alignment completes).
	for _, msg := range toProcess {
		if err := nr.handleMessage(ctx, msg, emit); err != nil {
			return err
		}
	}

	// If the aligner reports alignment, snapshot state and forward the barrier.
	if alignedBarrier != nil {
		if err := nr.handleAlignedBarrier(ctx, alignedBarrier, emit); err != nil {
			return err
		}
	}

	return nil
}

// handleIndexedWatermark processes a watermark signal using the WatermarkHolder
// to track per-input watermarks and only advance the output when the minimum
// across all inputs increases.
func (nr *NodeRuntime) handleIndexedWatermark(ctx context.Context, inputIndex int, wm *WatermarkSignal, emit Emitter) error {
	advanced := nr.watermarks.Update(inputIndex, *wm)
	if advanced == nil {
		return nil
	}

	// Notify the node of the watermark advancement.
	if err := nr.node.HandleWatermark(ctx, *advanced, emit); err != nil {
		return err
	}

	// Forward the advanced watermark to all outputs.
	msg := NewSignalMessage(SignalMessage{
		SignalType: SignalTypeWatermark,
		Watermark:  advanced,
	})
	for _, out := range nr.outputs {
		if err := out.Send(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

// handleAlignedBarrier snapshots state and forwards an aligned barrier to outputs.
func (nr *NodeRuntime) handleAlignedBarrier(ctx context.Context, barrier *BarrierSignal, emit Emitter) error {
	// Snapshot state via node callback.
	state, err := nr.node.HandleCheckpoint(ctx, *barrier)
	if err != nil {
		return err
	}

	// Forward barrier to all outputs.
	msg := NewSignalMessage(SignalMessage{
		SignalType: SignalTypeBarrier,
		Barrier:    barrier,
	})
	for _, out := range nr.outputs {
		if err := out.Send(ctx, msg); err != nil {
			return err
		}
	}

	// Report checkpoint completion to the controller if configured.
	if nr.completionCh != nil {
		nr.completionCh <- NodeCompletion{
			NodeID: nr.node.ID(),
			Epoch:  barrier.Epoch,
			State:  state,
		}
	}

	// If ThenStop, initiate graceful shutdown after forwarding.
	if barrier.ThenStop {
		nr.Stop()
	}

	return nil
}

// handleMessage dispatches a message to the appropriate handler based on type.
func (nr *NodeRuntime) handleMessage(ctx context.Context, msg Message, emit Emitter) error {
	if msg.IsData() {
		err := nr.node.ProcessMessage(ctx, *msg.Data, emit)
		if err != nil && nr.onError != nil {
			return nr.onError(nr.node.ID(), *msg.Data, err)
		}
		return err
	}

	if msg.IsSignal() && msg.Signal != nil {
		return nr.handleSignal(ctx, msg, emit)
	}
	return nil
}

// handleSignal handles in-band signal messages at the runtime level.
// Note: Barrier signals are handled by the BarrierAligner before reaching
// this method (via handleIndexedMessage → handleAlignedBarrier). This method
// only handles watermarks and stop signals.
func (nr *NodeRuntime) handleSignal(ctx context.Context, msg Message, emit Emitter) error {
	sig := msg.Signal

	switch sig.SignalType {
	case SignalTypeBarrier:
		// Barriers should not reach here — they are handled by the aligner.
		// This is a safety fallback for edge cases (e.g., no inputs).
		if sig.Barrier != nil {
			return nr.handleAlignedBarrier(ctx, sig.Barrier, emit)
		}

	case SignalTypeWatermark:
		// Watermarks are handled by handleIndexedWatermark (via the
		// WatermarkHolder) before reaching here. This case handles
		// watermarks that were buffered during barrier alignment —
		// they have already been accounted for by the WatermarkHolder,
		// so we skip them to avoid duplicate processing.
		return nil

	case SignalTypeStop:
		nr.activeInputs--
		if nr.activeInputs <= 0 {
			// Drain any in-flight async work before forwarding stop downstream.
			// This prevents message loss in operators like ConcurrentOperator
			// where goroutine workers may still be writing to output buffers.
			nr.drainIfDrainable(ctx)

			// All inputs have stopped — forward stop to outputs and shut down.
			for _, out := range nr.outputs {
				if err := out.Send(ctx, msg); err != nil {
					return err
				}
			}
			nr.Stop()
		}
		// If activeInputs > 0, continue processing from remaining inputs.
	}

	return nil
}

// operatorProvider is implemented by nodes that expose their underlying Operator.
type operatorProvider interface {
	Operator() Operator
}

// drainIfDrainable checks whether the node's underlying operator implements
// Drainable and, if so, calls Drain to flush in-flight work before stop
// propagation. Errors from Drain are logged but do not block shutdown.
func (nr *NodeRuntime) drainIfDrainable(ctx context.Context) {
	provider, ok := nr.node.(operatorProvider)
	if !ok {
		return
	}
	drainable, ok := provider.Operator().(Drainable)
	if !ok {
		return
	}
	if err := drainable.Drain(ctx); err != nil {
		// Log but don't block shutdown — context may already be cancelled.
		nr.err = err
	}
}

// handleControl handles out-of-band control messages.
func (nr *NodeRuntime) handleControl(ctx context.Context, ctrl ControlMessage, emit Emitter) {
	switch ctrl.Type {
	case ControlTypeCheckpointRequest:
		if ctrl.Checkpoint != nil {
			// Source nodes: inject a barrier into their outputs.
			barrier := NewBarrierSignal(ctrl.Checkpoint.Epoch, ctrl.Checkpoint.MinEpoch, ctrl.Checkpoint.ThenStop)
			msg := NewSignalMessage(barrier)

			// Snapshot state first.
			state, _ := nr.node.HandleCheckpoint(ctx, *msg.Signal.Barrier)

			// Forward barrier to outputs.
			for _, out := range nr.outputs {
				_ = out.Send(ctx, msg)
			}

			// Report checkpoint completion for the source node.
			if nr.completionCh != nil {
				nr.completionCh <- NodeCompletion{
					NodeID: nr.node.ID(),
					Epoch:  ctrl.Checkpoint.Epoch,
					State:  state,
				}
			}

			// If ThenStop, initiate graceful shutdown after forwarding.
			if ctrl.Checkpoint.ThenStop {
				nr.Stop()
			}
		}

	case ControlTypePause:
		nr.mu.Lock()
		nr.paused = true
		nr.mu.Unlock()

	case ControlTypeResume:
		nr.mu.Lock()
		nr.paused = false
		nr.mu.Unlock()

	case ControlTypeTerminate:
		// Handled by the caller (run loop exits).
	}
}

// isPaused returns whether the node is currently paused.
func (nr *NodeRuntime) isPaused() bool {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	return nr.paused
}

// waitForResume blocks until the node is resumed or the context is cancelled.
// It polls the control channel for resume/terminate messages.
func (nr *NodeRuntime) waitForResume(ctx context.Context) {
	for nr.isPaused() {
		select {
		case <-ctx.Done():
			return
		case ctrl, ok := <-nr.controlCh:
			if !ok {
				return
			}
			switch ctrl.Type {
			case ControlTypeResume:
				nr.mu.Lock()
				nr.paused = false
				nr.mu.Unlock()
				return
			case ControlTypeTerminate:
				nr.Stop()
				return
			}
		}
	}
}
