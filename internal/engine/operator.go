package engine

import (
	"context"
	"time"
)

// Operator is the user-facing interface for creating processing nodes.
// It provides a simpler API than the low-level Node interface, with lifecycle
// hooks and a Collector for emitting output messages. Wrap an Operator in an
// OperatorNode to use it with the runtime.
type Operator interface {
	// ProcessMessage handles a single data message. The implementation
	// should process the message and emit zero or more output messages
	// via the provided Collector.
	ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error

	// HandleWatermark is called when the node's input watermark advances.
	// Implementations can use this to trigger time-based operations.
	HandleWatermark(ctx context.Context, watermark WatermarkSignal, collector Collector) error

	// HandleCheckpoint is called when a checkpoint barrier arrives and the
	// operator should snapshot its state.
	HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error)

	// OnStart is called once when the node starts processing.
	OnStart(ctx context.Context) error

	// OnClose is called once when the node stops processing.
	OnClose(ctx context.Context) error
}

// Drainable is an optional interface that operators can implement to flush
// in-flight work before the stop signal propagates downstream. The runtime
// calls Drain() during graceful shutdown to ensure async work completes and
// results are emitted before downstream buffers close.
type Drainable interface {
	// Drain blocks until all in-flight work is complete and results have
	// been emitted to downstream buffers, then returns. Must respect
	// context cancellation and be idempotent.
	Drain(ctx context.Context) error
}

// TickOperator extends Operator with periodic tick support. Operators that
// need periodic callbacks should implement this interface.
type TickOperator interface {
	Operator

	// TickInterval returns the interval between tick callbacks.
	TickInterval() time.Duration

	// HandleTick is called periodically at the configured interval.
	HandleTick(ctx context.Context, collector Collector) error
}

// Source produces data into the stream processor graph. Unlike Operator,
// a Source has a long-running Run method that pushes data into the graph.
type Source interface {
	// Run is the main loop that produces data. It should emit messages via
	// the Collector and return when ctx is cancelled or the source is done.
	Run(ctx context.Context, collector Collector) error
}

// Collector allows operators to emit output messages, register timers, and
// report errors. The runtime provides a Collector implementation that routes
// messages to the appropriate output buffers.
type Collector interface {
	// Emit sends a data message to the default output port.
	Emit(msg DataMessage) error

	// EmitToPort sends a data message to a specific named output port.
	EmitToPort(port string, msg DataMessage) error

	// EmitError routes a failed message to the pipeline's error handler.
	// The behavior depends on the configured ErrorStrategy (SkipAndLog,
	// DeadLetter, or FailPipeline). This allows operators to explicitly
	// report errors for specific messages without returning an error from
	// ProcessMessage.
	EmitError(msg DataMessage, err error)

	// RegisterTimer schedules a named timer to fire at the given time.
	// When the timer fires, the operator's HandleTimer method is called
	// (if the operator implements TimerOperator). No-ops if the operator
	// does not support timers.
	RegisterTimer(name string, fireAt time.Time)
}

// TickHandler is an optional interface that Node implementations can provide
// to receive periodic tick callbacks from the runtime.
type TickHandler interface {
	HandleTick(ctx context.Context, emit Emitter) error
}

// --- Collector implementation ---

// runtimeCollector implements Collector by wrapping the runtime's output buffers.
type runtimeCollector struct {
	ctx     context.Context
	outputs []*Buffer
	// portOutputs maps port names to specific output buffers for port-based routing.
	portOutputs map[string][]*Buffer
	// timerMgr is the timer manager for registering timers. May be nil.
	timerMgr *timerManager
	// nodeID identifies the owning node (used for error reporting).
	nodeID string
	// onError is called when EmitError is invoked. May be nil.
	onError errorHandler
}

// Emit sends a data message to all default outputs.
func (c *runtimeCollector) Emit(msg DataMessage) error {
	m := NewDataMessage(msg.Key, msg.Value, msg.EventTime)
	for _, out := range c.outputs {
		if err := out.Send(c.ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// EmitToPort sends a data message to outputs mapped to the given port name.
// If no port mapping exists, it falls back to emitting to all outputs.
func (c *runtimeCollector) EmitToPort(port string, msg DataMessage) error {
	m := NewDataMessage(msg.Key, msg.Value, msg.EventTime)
	if outs, ok := c.portOutputs[port]; ok {
		for _, out := range outs {
			if err := out.Send(c.ctx, m); err != nil {
				return err
			}
		}
		return nil
	}
	// Fallback: emit to all outputs.
	return c.Emit(msg)
}

// RegisterTimer schedules a named timer via the timer manager.
func (c *runtimeCollector) RegisterTimer(name string, fireAt time.Time) {
	if c.timerMgr != nil {
		c.timerMgr.Register(name, fireAt)
	}
}

// EmitError routes a failed message to the pipeline's error handler.
func (c *runtimeCollector) EmitError(msg DataMessage, err error) {
	if c.onError != nil {
		_ = c.onError(c.nodeID, msg, err)
	}
}

// --- OperatorNode adapter ---

// OperatorNode adapts an Operator to the Node interface so it can be used
// with the NodeRuntime. It also implements TickHandler if the wrapped
// Operator implements TickOperator.
type OperatorNode struct {
	id          string
	operator    Operator
	outputs     []*Buffer
	portOutputs map[string][]*Buffer
	timerMgr    *timerManager
	onError     errorHandler
}

// NewOperatorNode creates a new OperatorNode wrapping the given Operator.
// If the operator implements TimerOperator, a timer manager is created
// to support timer registration and checkpoint.
func NewOperatorNode(id string, op Operator) *OperatorNode {
	n := &OperatorNode{
		id:       id,
		operator: op,
	}
	if _, ok := op.(TimerOperator); ok {
		n.timerMgr = newTimerManager()
	}
	return n
}

// ID returns the unique identifier for this node.
func (n *OperatorNode) ID() string { return n.id }

// newCollector creates a runtimeCollector for this node with the appropriate
// timer manager reference.
func (n *OperatorNode) newCollector(ctx context.Context) *runtimeCollector {
	return &runtimeCollector{ctx: ctx, outputs: n.outputs, portOutputs: n.portOutputs, timerMgr: n.timerMgr, nodeID: n.id, onError: n.onError}
}

// ProcessMessage delegates to the wrapped Operator.
func (n *OperatorNode) ProcessMessage(ctx context.Context, msg DataMessage, emit Emitter) error {
	return n.operator.ProcessMessage(ctx, msg, n.newCollector(ctx))
}

// HandleWatermark delegates to the wrapped Operator.
func (n *OperatorNode) HandleWatermark(ctx context.Context, watermark WatermarkSignal, emit Emitter) error {
	return n.operator.HandleWatermark(ctx, watermark, n.newCollector(ctx))
}

// HandleCheckpoint delegates to the wrapped Operator. If the operator supports
// timers, the checkpoint includes timer registration state.
func (n *OperatorNode) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	opState, err := n.operator.HandleCheckpoint(ctx, barrier)
	if err != nil {
		return nil, err
	}
	if n.timerMgr == nil {
		return opState, nil
	}
	timerState := n.timerMgr.Snapshot()
	return encodeTimerCheckpoint(opState, timerState), nil
}

// RestoreTimerState restores the timer manager state from a checkpoint.
// The data should be the combined state returned by HandleCheckpoint.
// Returns the operator-only state for the caller to restore separately.
func (n *OperatorNode) RestoreTimerState(data []byte) ([]byte, error) {
	if n.timerMgr == nil || len(data) == 0 {
		return data, nil
	}
	opState, timerState, err := decodeTimerCheckpoint(data)
	if err != nil {
		return nil, err
	}
	if err := n.timerMgr.Restore(timerState); err != nil {
		return nil, err
	}
	return opState, nil
}

// HandleTick delegates to the wrapped Operator if it implements TickOperator.
func (n *OperatorNode) HandleTick(ctx context.Context, emit Emitter) error {
	if tickOp, ok := n.operator.(TickOperator); ok {
		return tickOp.HandleTick(ctx, n.newCollector(ctx))
	}
	return nil
}

// HandleTimer delegates to the wrapped Operator if it implements TimerOperator.
func (n *OperatorNode) HandleTimer(ctx context.Context, name string, fireAt time.Time, emit Emitter) error {
	if timerOp, ok := n.operator.(TimerOperator); ok {
		return timerOp.HandleTimer(ctx, name, fireAt, n.newCollector(ctx))
	}
	return nil
}

// getTimerManager returns the timer manager, or nil if timers are not supported.
func (n *OperatorNode) getTimerManager() *timerManager {
	return n.timerMgr
}

// SetOutputs sets the output buffers for this node. This is called by the
// pipeline builder before the node starts.
func (n *OperatorNode) SetOutputs(outputs []*Buffer) {
	n.outputs = outputs
}

// SetPortOutputs sets the port-based output buffer mapping. This enables
// EmitToPort routing for operators like RouterOperator.
func (n *OperatorNode) SetPortOutputs(portOutputs map[string][]*Buffer) {
	n.portOutputs = portOutputs
}

// SetOnError sets the error handler for this node's collector.
func (n *OperatorNode) SetOnError(handler errorHandler) {
	n.onError = handler
}

// Operator returns the underlying Operator.
func (n *OperatorNode) Operator() Operator {
	return n.operator
}

// --- SourceNode adapter ---

// SourceNode adapts a Source to the Node interface. Sources are long-running
// and produce data via their Run method rather than processing input messages.
type SourceNode struct {
	id          string
	source      Source
	outputs     []*Buffer
	portOutputs map[string][]*Buffer
	onError     errorHandler
}

// NewSourceNode creates a new SourceNode wrapping the given Source.
func NewSourceNode(id string, src Source) *SourceNode {
	return &SourceNode{
		id:     id,
		source: src,
	}
}

// ID returns the unique identifier for this node.
func (n *SourceNode) ID() string { return n.id }

// ProcessMessage is a no-op for sources (they don't receive input).
func (n *SourceNode) ProcessMessage(ctx context.Context, msg DataMessage, emit Emitter) error {
	return nil
}

// HandleWatermark is a no-op for sources.
func (n *SourceNode) HandleWatermark(ctx context.Context, watermark WatermarkSignal, emit Emitter) error {
	return nil
}

// HandleCheckpoint snapshots state (sources are stateless by default).
func (n *SourceNode) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	return nil, nil
}

// SetOutputs sets the output buffers for this node.
func (n *SourceNode) SetOutputs(outputs []*Buffer) {
	n.outputs = outputs
}

// SetPortOutputs sets the port-based output buffer mapping.
func (n *SourceNode) SetPortOutputs(portOutputs map[string][]*Buffer) {
	n.portOutputs = portOutputs
}

// RunSource starts the source's Run method. This should be called in a
// separate goroutine alongside the NodeRuntime.
// SetOnError sets the error handler for this node's collector.
func (n *SourceNode) SetOnError(handler errorHandler) {
	n.onError = handler
}

func (n *SourceNode) RunSource(ctx context.Context) error {
	collector := &runtimeCollector{ctx: ctx, outputs: n.outputs, portOutputs: n.portOutputs, nodeID: n.id, onError: n.onError}
	return n.source.Run(ctx, collector)
}

// Source returns the underlying Source.
func (n *SourceNode) Source() Source {
	return n.source
}

// --- Built-in operators ---

// BaseOperator provides default no-op implementations of Operator methods.
// Embed this in custom operators to avoid implementing every method.
type BaseOperator struct{}

func (BaseOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	return nil
}
func (BaseOperator) HandleWatermark(ctx context.Context, watermark WatermarkSignal, collector Collector) error {
	return nil
}
func (BaseOperator) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	return nil, nil
}
func (BaseOperator) OnStart(ctx context.Context) error { return nil }
func (BaseOperator) OnClose(ctx context.Context) error { return nil }

// MapOperator transforms each input message using a mapping function.
// The mapping function receives the input value and returns the transformed value.
type MapOperator[In any, Out any] struct {
	BaseOperator
	MapFn func(In) Out
}

// ProcessMessage applies the mapping function and emits the result.
func (m *MapOperator[In, Out]) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	input, ok := msg.Value.(In)
	if !ok {
		return nil // skip messages with wrong type
	}
	output := m.MapFn(input)
	return collector.Emit(DataMessage{
		Key:       msg.Key,
		Value:     output,
		EventTime: msg.EventTime,
	})
}

// FilterOperator drops messages that fail a predicate function.
// Only messages for which the predicate returns true are emitted.
type FilterOperator[T any] struct {
	BaseOperator
	Predicate func(T) bool
}

// ProcessMessage applies the predicate and emits the message if it passes.
func (f *FilterOperator[T]) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	input, ok := msg.Value.(T)
	if !ok {
		return nil // skip messages with wrong type
	}
	if f.Predicate(input) {
		return collector.Emit(msg)
	}
	return nil
}

// FanOutOperator duplicates each input message to all output ports.
// This is a passthrough operator — the runtime's default emission already
// sends to all outputs, so FanOutOperator simply re-emits the message.
type FanOutOperator struct {
	BaseOperator
}

// ProcessMessage emits the message to all outputs.
func (f *FanOutOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	return collector.Emit(msg)
}

// MergeOperator merges multiple input streams into a single output stream.
// It is a passthrough — every input message is emitted unchanged.
type MergeOperator struct {
	BaseOperator
}

// ProcessMessage forwards the message to the output.
func (m *MergeOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	return collector.Emit(msg)
}

// RouterOperator routes messages to different output ports based on a routing
// function. Each message is sent to exactly one port determined by the RouteFn.
// If RouteFn returns an empty string, the message is sent to DefaultPort (if
// configured) or dropped silently.
type RouterOperator struct {
	BaseOperator
	// RouteFn determines which output port a message should be routed to.
	// It receives the message and returns a port name string.
	// Return "" to indicate no match (will use DefaultPort or drop).
	RouteFn func(DataMessage) string
	// DefaultPort is the port name to use when RouteFn returns "".
	// If empty, unmatched messages are dropped.
	DefaultPort string
}

// ProcessMessage routes the message to the port returned by RouteFn.
func (r *RouterOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	port := r.RouteFn(msg)
	if port == "" {
		if r.DefaultPort != "" {
			port = r.DefaultPort
		} else {
			return nil // drop unmatched message
		}
	}
	return collector.EmitToPort(port, msg)
}
