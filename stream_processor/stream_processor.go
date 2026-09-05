// Package stream_processor provides the primary API for building embedded stream
// processing pipelines. It exposes the user-facing abstractions — operators,
// sources, collectors, pipelines, and state management — while hiding the
// runtime plumbing (buffers, barrier alignment, watermarks) in the internal
// engine package.
//
// # Quick start
//
// Build a pipeline with the fluent GraphBuilder, then call Start:
//
//	pipeline, err := stream_processor.NewGraphBuilder("my-pipeline").
//	    AddSource("in", mySource).
//	    AddOperator("transform", myOperator).
//	    AddSink("out", mySink).
//	    Connect("in", "transform").
//	    Connect("transform", "out").
//	    Build()
//
//	ctx := context.Background()
//	pipeline.Start(ctx)
//	defer pipeline.Stop(ctx)
//	pipeline.Wait()
//
// # Implementing operators
//
// The simplest way to create operators is via the [nodes] package, which
// provides functional constructors (nodes.Map, nodes.Filter, nodes.SourceFunc,
// etc.). For stateful or lifecycle-aware operators, implement the [Operator]
// interface and optionally embed [BaseOperator] for default no-op methods.
//
// # Lifecycle
//
// Pipelines progress through well-defined states:
// Initializing → Running → (Paused) → Stopping → Stopped.
// Use [Pipeline.Stop] for graceful drain-and-checkpoint, [Pipeline.Terminate]
// for immediate shutdown, and [Pipeline.Pause]/[Pipeline.Resume] to gate
// processing.
//
// See the docs/ directory for full architecture and how-to guides.
package stream_processor

import "github.com/portpowered/go-stream-processor/internal/engine"

// ---------------------------------------------------------------------------
// Message types (re-exported from internal/engine)
// ---------------------------------------------------------------------------

// DataMessage carries a user-defined payload through the graph.
type DataMessage = engine.DataMessage

// WatermarkSignal carries event-time progress information. Watermarks flow
// in-band and allow downstream nodes to reason about event-time completeness.
type WatermarkSignal = engine.WatermarkSignal

// BarrierSignal carries checkpoint barrier information for the Chandy-Lamport
// snapshotting algorithm. Operators receive this in HandleCheckpoint.
type BarrierSignal = engine.BarrierSignal

// ---------------------------------------------------------------------------
// Core user interfaces
// ---------------------------------------------------------------------------

// Operator is the primary interface for building processing nodes. Implement
// this interface to define custom data transformations.
//
// For simple transformations that don't need lifecycle hooks, state, or
// watermark handling, embed [BaseOperator] to inherit no-op defaults and
// only override ProcessMessage:
//
//	type MyOp struct {
//	    stream_processor.BaseOperator
//	}
//
//	func (o *MyOp) ProcessMessage(ctx context.Context, msg stream_processor.DataMessage, c stream_processor.Collector) error {
//	    // transform msg and emit results
//	    return c.Emit(stream_processor.DataMessage{Key: msg.Key, Value: transform(msg.Value)})
//	}
//
// For the simplest case — a stateless transformation — use [nodes.Map] or
// [nodes.Filter] from the nodes package.
type Operator = engine.Operator

// Source produces data into the stream processor graph. Implement Run to
// push messages until ctx is cancelled or the source is exhausted.
//
// For a simple function-based source, use [nodes.SourceFunc] from the nodes
// package.
type Source = engine.Source

// Collector allows operators and sources to emit output messages, register
// timers, and report per-message errors. The runtime provides a Collector
// that routes messages to downstream node buffers.
type Collector = engine.Collector

// BaseOperator provides default no-op implementations of all [Operator] methods.
// Embed it in custom operators to avoid boilerplate:
//
//	type MyOp struct {
//	    stream_processor.BaseOperator
//	}
//
// Only override the methods you need.
type BaseOperator = engine.BaseOperator

// TickOperator extends [Operator] with periodic tick support. Implement this
// if your operator needs a recurring background callback (e.g., to flush
// buffered state periodically).
type TickOperator = engine.TickOperator

// TimerOperator extends [Operator] with event-time timer support. Operators
// that register timers via [Collector.RegisterTimer] should implement this
// interface to receive the callback when the timer fires.
type TimerOperator = engine.TimerOperator

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

// Pipeline is an executable stream processing graph. Obtain one via
// [GraphBuilder.Build], then drive it with [Pipeline.Start].
type Pipeline = engine.Pipeline

// PipelineState represents the current lifecycle state of a [Pipeline].
type PipelineState = engine.PipelineState

const (
	// PipelineInitializing is the state before Start is called.
	PipelineInitializing = engine.PipelineInitializing
	// PipelineRunning is the state after Start has launched all goroutines.
	PipelineRunning = engine.PipelineRunning
	// PipelinePaused is the state after Pause — nodes are alive but idle.
	PipelinePaused = engine.PipelinePaused
	// PipelineStopping is the state after Stop — a drain barrier is propagating.
	PipelineStopping = engine.PipelineStopping
	// PipelineStopped is the terminal state after a graceful Stop.
	PipelineStopped = engine.PipelineStopped
	// PipelineTerminated is the terminal state after an immediate Terminate.
	PipelineTerminated = engine.PipelineTerminated
	// PipelineError is the terminal state when the pipeline encountered a fatal error.
	PipelineError = engine.PipelineError
)

// ---------------------------------------------------------------------------
// GraphBuilder
// ---------------------------------------------------------------------------

// GraphBuilder provides a fluent API for constructing stream processing
// pipelines. Create one with [NewGraphBuilder], add nodes and connections,
// then call [GraphBuilder.Build] to get an executable [Pipeline].
type GraphBuilder = engine.GraphBuilder

// NewGraphBuilder creates a new graph builder with the given pipeline name.
//
// Example:
//
//	b := stream_processor.NewGraphBuilder("my-pipeline")
//	b.AddSource("source", src).
//	    AddOperator("transform", op).
//	    AddSink("sink", sink).
//	    Connect("source", "transform").
//	    Connect("transform", "sink")
//	pipeline, err := b.Build()
var NewGraphBuilder = engine.NewGraphBuilder

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

// StateStore provides a key-value state abstraction for stateful operators.
// Use it inside HandleCheckpoint to snapshot and restore keyed state.
type StateStore = engine.StateStore

// InMemoryStateStore is a thread-safe in-memory implementation of [StateStore].
type InMemoryStateStore = engine.InMemoryStateStore

// NewInMemoryStateStore creates a new empty in-memory state store.
var NewInMemoryStateStore = engine.NewInMemoryStateStore

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

// NodeCompletion is a report from a node that it has completed a checkpoint,
// carrying the node's serialized state snapshot.
type NodeCompletion = engine.NodeCompletion

// CheckpointBackend provides pluggable persistence for checkpoint snapshots.
type CheckpointBackend = engine.CheckpointBackend

// LocalDiskBackend implements [CheckpointBackend] using the local filesystem.
// Each epoch gets a subdirectory; each node's state is written atomically.
type LocalDiskBackend = engine.LocalDiskBackend

// NewLocalDiskBackend creates a [LocalDiskBackend] that writes to baseDir.
var NewLocalDiskBackend = engine.NewLocalDiskBackend

// CheckpointManager orchestrates state serialization across all nodes,
// integrating with the Controller's OnCheckpointComplete callback.
type CheckpointManager = engine.CheckpointManager

// CheckpointManagerConfig configures a [CheckpointManager].
type CheckpointManagerConfig = engine.CheckpointManagerConfig

// NewCheckpointManager creates a new CheckpointManager with the given config.
var NewCheckpointManager = engine.NewCheckpointManager

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

// ErrorStrategy determines how the pipeline handles errors from operators.
type ErrorStrategy = engine.ErrorStrategy

const (
	// ErrorStrategySkipAndLog skips the failed message and logs the error.
	// This is the default strategy.
	ErrorStrategySkipAndLog = engine.ErrorStrategySkipAndLog
	// ErrorStrategyDeadLetter routes failed messages to a dead-letter sink.
	ErrorStrategyDeadLetter = engine.ErrorStrategyDeadLetter
	// ErrorStrategyFailPipeline stops the pipeline on the first error.
	ErrorStrategyFailPipeline = engine.ErrorStrategyFailPipeline
)

// ErrorConfig configures error handling for a pipeline.
type ErrorConfig = engine.ErrorConfig

// DeadLetterMessage wraps a failed message with error context. It is delivered
// as the Value of a DataMessage to the dead-letter sink operator.
type DeadLetterMessage = engine.DeadLetterMessage

// ---------------------------------------------------------------------------
// Built-in timer-based operators
// ---------------------------------------------------------------------------

// SleepOperator delays each message by a fixed duration using event-time
// timers. This is non-blocking and checkpoint-safe.
type SleepOperator = engine.SleepOperator

// NewSleepOperator creates a SleepOperator with the given delay.
var NewSleepOperator = engine.NewSleepOperator

// DebounceOperator suppresses repeated signals within a configurable window,
// emitting only the last message after the window expires for each key.
type DebounceOperator = engine.DebounceOperator

// NewDebounceOperator creates a DebounceOperator with the given window.
var NewDebounceOperator = engine.NewDebounceOperator

// DeduplicateOperator drops messages with duplicate keys within a window,
// using timers to expire old keys so later messages are not suppressed.
type DeduplicateOperator = engine.DeduplicateOperator

// NewDeduplicateOperator creates a DeduplicateOperator with the given window.
var NewDeduplicateOperator = engine.NewDeduplicateOperator

// ---------------------------------------------------------------------------
// Testing utilities
// ---------------------------------------------------------------------------

// TestHarness runs a pipeline with a configurable timeout and detects
// deadlocks by inspecting goroutine state when the pipeline stalls.
type TestHarness = engine.TestHarness

// TestHarnessResult contains the outcome of a [TestHarness] run.
type TestHarnessResult = engine.TestHarnessResult

// DefaultBufferCapacity is the default buffer capacity between nodes.
const DefaultBufferCapacity = engine.DefaultBufferCapacity

// DefaultPort is the port name used when no explicit port is specified.
const DefaultPort = engine.DefaultPort
