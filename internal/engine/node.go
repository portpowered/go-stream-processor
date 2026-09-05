package engine

import "context"

// Node is the fundamental processing unit in the stream processor graph.
// Each node runs as a goroutine and processes data messages, watermarks,
// and checkpoint barriers. Implementations define the processing logic;
// the runtime handles scheduling, signal routing, and lifecycle management.
type Node interface {
	// ID returns the unique identifier for this node within the graph.
	ID() string

	// ProcessMessage handles a single data message. The implementation
	// should process the message and emit zero or more output messages
	// via the provided Emitter.
	ProcessMessage(ctx context.Context, msg DataMessage, emit Emitter) error

	// HandleWatermark is called when the node's input watermark advances.
	// Implementations can use this to trigger time-based operations such
	// as flushing windows or emitting delayed results.
	HandleWatermark(ctx context.Context, watermark WatermarkSignal, emit Emitter) error

	// HandleCheckpoint is called when a checkpoint barrier arrives and the
	// node should snapshot its state. The returned byte slice is the
	// serialized state that will be persisted for recovery.
	HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error)
}

// Emitter allows nodes to produce output messages. The runtime provides an
// Emitter implementation that routes messages to the appropriate output buffers.
type Emitter interface {
	// Emit sends a message to the default output port.
	Emit(ctx context.Context, msg Message) error
}

// NodeType classifies a node's role in the graph for validation purposes.
type NodeType int

const (
	// NodeTypeSource is a node that produces data (no inputs expected).
	NodeTypeSource NodeType = iota
	// NodeTypeOperator is a node that transforms data (inputs and outputs).
	NodeTypeOperator
	// NodeTypeSink is a node that consumes data (no outputs expected).
	NodeTypeSink
)
