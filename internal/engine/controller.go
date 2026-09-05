package engine

import (
	"context"
	"sync"
	"time"
)

// CheckpointStatus represents the completion state of a checkpoint epoch.
type CheckpointStatus int

const (
	// CheckpointInProgress indicates the checkpoint barrier is still propagating.
	CheckpointInProgress CheckpointStatus = iota
	// CheckpointComplete indicates all nodes have completed the checkpoint.
	CheckpointComplete
)

// CheckpointInfo holds metadata about a tracked checkpoint epoch.
type CheckpointInfo struct {
	// Epoch is the checkpoint epoch number.
	Epoch uint64
	// Status is the current completion state.
	Status CheckpointStatus
	// StartedAt is when the controller began tracking this epoch.
	StartedAt time.Time
	// CompletedAt is when all nodes reported completion. Zero if incomplete.
	CompletedAt time.Time
	// CompletedNodes tracks which nodes have reported completion.
	CompletedNodes map[string]bool
	// NodeStates holds the serialized state snapshot from each node's checkpoint.
	// Nodes that return nil state have a nil entry (not omitted).
	NodeStates map[string][]byte
	// ThenStop indicates the pipeline should stop after this checkpoint.
	ThenStop bool
}

// NodeCompletion is a report from a node that it has completed a checkpoint.
type NodeCompletion struct {
	// NodeID is the reporting node's identifier.
	NodeID string
	// Epoch is the checkpoint epoch that was completed.
	Epoch uint64
	// State is the serialized state snapshot from the node.
	State []byte
}

// NodeStopped is a report from a node that it has stopped (received Stop signal).
type NodeStopped struct {
	// NodeID is the reporting node's identifier.
	NodeID string
}

// Controller tracks checkpoint barrier progress across all nodes in a graph
// and detects quiescence — the state where a checkpoint has fully propagated
// and all nodes have reported completion.
type Controller struct {
	mu sync.Mutex

	// nodeIDs is the set of all node IDs in the graph.
	nodeIDs map[string]bool
	// sourceIDs is the set of source node IDs.
	sourceIDs map[string]bool
	// sinkIDs is the set of sink node IDs.
	sinkIDs map[string]bool

	// checkpoints tracks in-progress and completed checkpoints by epoch.
	checkpoints map[uint64]*CheckpointInfo

	// stoppedNodes tracks which nodes have reported stopping.
	stoppedNodes map[string]bool

	// completionCh receives checkpoint completion reports from nodes.
	completionCh chan NodeCompletion
	// stoppedCh receives stop reports from nodes.
	stoppedCh chan NodeStopped

	// onCheckpointComplete is called when a checkpoint epoch completes.
	onCheckpointComplete func(epoch uint64, info *CheckpointInfo)
	// onQuiescence is called when the graph reaches quiescence.
	onQuiescence func()

	// quiescent indicates the graph has reached quiescence.
	quiescent bool

	// done signals the controller to stop.
	cancel context.CancelFunc
	done   chan struct{}
}

// ControllerConfig configures a Controller.
type ControllerConfig struct {
	// NodeIDs is the set of all node IDs in the graph.
	NodeIDs []string
	// SourceIDs is the set of source node IDs.
	SourceIDs []string
	// SinkIDs is the set of sink node IDs.
	SinkIDs []string
	// OnCheckpointComplete is called when a checkpoint epoch fully completes.
	OnCheckpointComplete func(epoch uint64, info *CheckpointInfo)
	// OnQuiescence is called when the graph reaches quiescence (all sources
	// stopped, all nodes drained and reported final checkpoint).
	OnQuiescence func()
	// CompletionBufferSize is the capacity of the completion channel. Defaults to 64.
	CompletionBufferSize int
}

// NewController creates a new Controller for tracking checkpoint progress.
func NewController(cfg ControllerConfig) *Controller {
	bufSize := cfg.CompletionBufferSize
	if bufSize <= 0 {
		bufSize = 64
	}

	nodeIDs := make(map[string]bool, len(cfg.NodeIDs))
	for _, id := range cfg.NodeIDs {
		nodeIDs[id] = true
	}
	sourceIDs := make(map[string]bool, len(cfg.SourceIDs))
	for _, id := range cfg.SourceIDs {
		sourceIDs[id] = true
	}
	sinkIDs := make(map[string]bool, len(cfg.SinkIDs))
	for _, id := range cfg.SinkIDs {
		sinkIDs[id] = true
	}

	return &Controller{
		nodeIDs:              nodeIDs,
		sourceIDs:            sourceIDs,
		sinkIDs:              sinkIDs,
		checkpoints:          make(map[uint64]*CheckpointInfo),
		stoppedNodes:         make(map[string]bool),
		completionCh:         make(chan NodeCompletion, bufSize),
		stoppedCh:            make(chan NodeStopped, bufSize),
		onCheckpointComplete: cfg.OnCheckpointComplete,
		onQuiescence:         cfg.OnQuiescence,
		done:                 make(chan struct{}),
	}
}

// CompletionChan returns the channel for nodes to report checkpoint completion.
func (c *Controller) CompletionChan() chan<- NodeCompletion {
	return c.completionCh
}

// StoppedChan returns the channel for nodes to report they have stopped.
func (c *Controller) StoppedChan() chan<- NodeStopped {
	return c.stoppedCh
}

// Start launches the controller's tracking goroutine.
func (c *Controller) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.run(runCtx)
}

// Stop requests the controller to shut down.
func (c *Controller) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done
}

// InitiateCheckpoint begins tracking a new checkpoint epoch. It sends
// checkpoint requests to the provided source runtimes.
func (c *Controller) InitiateCheckpoint(ctx context.Context, epoch uint64, thenStop bool, sourceRuntimes []*NodeRuntime) error {
	c.mu.Lock()
	c.checkpoints[epoch] = &CheckpointInfo{
		Epoch:          epoch,
		Status:         CheckpointInProgress,
		StartedAt:      time.Now(),
		CompletedNodes: make(map[string]bool),
		NodeStates:     make(map[string][]byte),
		ThenStop:       thenStop,
	}
	c.mu.Unlock()

	// Send checkpoint request to all source nodes.
	ctrl := NewCheckpointRequestControl(epoch, 0, thenStop)
	for _, rt := range sourceRuntimes {
		if err := rt.SendControl(ctx, ctrl); err != nil {
			return err
		}
	}
	return nil
}

// GetCheckpoint returns a copy of the checkpoint info for the given epoch.
func (c *Controller) GetCheckpoint(epoch uint64) (CheckpointInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, ok := c.checkpoints[epoch]
	if !ok {
		return CheckpointInfo{}, false
	}
	// Return a copy with copied maps.
	cp := *info
	cp.CompletedNodes = make(map[string]bool, len(info.CompletedNodes))
	for k, v := range info.CompletedNodes {
		cp.CompletedNodes[k] = v
	}
	cp.NodeStates = make(map[string][]byte, len(info.NodeStates))
	for k, v := range info.NodeStates {
		cp.NodeStates[k] = v
	}
	return cp, true
}

// IsQuiescent returns whether the graph has reached quiescence.
func (c *Controller) IsQuiescent() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.quiescent
}

// run is the main controller loop that processes completion and stop reports.
func (c *Controller) run(ctx context.Context) {
	defer close(c.done)

	for {
		select {
		case <-ctx.Done():
			return

		case completion, ok := <-c.completionCh:
			if !ok {
				return
			}
			c.handleCompletion(completion)

		case stopped, ok := <-c.stoppedCh:
			if !ok {
				return
			}
			c.handleNodeStopped(stopped)
		}
	}
}

// handleCompletion processes a checkpoint completion report from a node.
func (c *Controller) handleCompletion(completion NodeCompletion) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, ok := c.checkpoints[completion.Epoch]
	if !ok {
		// Auto-create tracking for this epoch if we receive a completion
		// before InitiateCheckpoint was called (e.g., barrier injected externally).
		info = &CheckpointInfo{
			Epoch:          completion.Epoch,
			Status:         CheckpointInProgress,
			StartedAt:      time.Now(),
			CompletedNodes: make(map[string]bool),
			NodeStates:     make(map[string][]byte),
		}
		c.checkpoints[completion.Epoch] = info
	}

	if info.Status == CheckpointComplete {
		// Already completed — ignore duplicate report.
		return
	}

	info.CompletedNodes[completion.NodeID] = true
	info.NodeStates[completion.NodeID] = completion.State

	// Check if all nodes have reported.
	if len(info.CompletedNodes) == len(c.nodeIDs) {
		info.Status = CheckpointComplete
		info.CompletedAt = time.Now()

		if c.onCheckpointComplete != nil {
			c.onCheckpointComplete(completion.Epoch, info)
		}

		// Check quiescence after checkpoint completion.
		c.checkQuiescenceLocked()
	}
}

// handleNodeStopped processes a stop report from a node.
func (c *Controller) handleNodeStopped(stopped NodeStopped) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stoppedNodes[stopped.NodeID] = true
	c.checkQuiescenceLocked()
}

// checkQuiescenceLocked checks whether the graph has reached quiescence.
// Must be called with c.mu held.
//
// Quiescence is detected when:
// 1. All source nodes have reported stopping.
// 2. All nodes have reported stopping.
// 3. There exists at least one completed checkpoint (ensuring state consistency).
func (c *Controller) checkQuiescenceLocked() {
	if c.quiescent {
		return
	}

	// All nodes must have stopped.
	if len(c.stoppedNodes) < len(c.nodeIDs) {
		return
	}

	// At least one checkpoint must have completed to ensure consistency.
	hasCompleted := false
	for _, info := range c.checkpoints {
		if info.Status == CheckpointComplete {
			hasCompleted = true
			break
		}
	}
	if !hasCompleted {
		return
	}

	c.quiescent = true
	if c.onQuiescence != nil {
		c.onQuiescence()
	}
}
