# Architecture

This document describes the internal architecture of `go-stream-processor` for contributors and advanced users who want to understand how the runtime works.

---

## Overview

The library models a stream processing job as a **directed acyclic graph (DAG)** of nodes. Each node runs on its own goroutine and communicates with adjacent nodes through **bounded channels (Buffers)**. The runtime routes two kinds of messages through those channels:

| Kind | Type | Purpose |
|---|---|---|
| **Data** | `DataMessage` | User payload: key, value, event timestamp |
| **Signal** | `SignalMessage` | In-band control: barrier, watermark, stop |

Data messages carry user payloads. Signal messages piggyback on the same channel so that signals respect the same ordering guarantees as data.

---

## Package structure

```
internal/engine/          Runtime plumbing — not exposed to users
  message.go              Message union type (data / signal)
  node.go                 Low-level Node and Emitter interfaces
  buffer.go               Bounded channel between two nodes
  graph.go                DAG container and validation (cycle detection)
  control.go              Out-of-band control messages (pause, resume, checkpoint)
  barrier_aligner.go      Chandy-Lamport barrier alignment for multi-input nodes
  watermark.go            Per-input watermark tracking, minimum propagation
  controller.go           Checkpoint progress tracker, quiescence detection
  runtime.go              NodeRuntime — the goroutine loop for one node
  timer.go                Event-time timer management

stream_processor/         User-facing API — import this
  stream_processor.go     Type aliases + re-exported constructors

nodes/                    Prebuilt operators — import this
  nodes.go                Map, Filter, FanOut, Merge, Router, Concurrent, SourceFunc, SinkFunc
```

---

## Node types

| Type | Role | Inputs | Outputs |
|---|---|---|---|
| **Source** | Produces data | None | ≥ 1 |
| **Operator** | Transforms data | ≥ 1 | ≥ 1 |
| **Sink** | Consumes data | ≥ 1 | None |

Sources run a long-lived `Run(ctx, collector)` loop. When `Run` returns, the runtime injects a **stop signal** into each output buffer, propagating shutdown downstream through the graph.

---

## Message flow

```
Source ──[Buffer]──► Operator ──[Buffer]──► Sink
         data+signals            data+signals
```

Each `Buffer` is a bounded `chan Message`. When the buffer is full, `Send` blocks, creating natural **backpressure** from slow consumers to fast producers.

The `NodeRuntime` for each node runs a `select` loop over:
- All input buffers (merged into a single `indexedMessage` channel)
- An out-of-band control channel (pause, resume, terminate)
- An optional periodic tick timer
- Event-time timers (for operators that register them)

---

## Barrier-based checkpointing (Chandy-Lamport)

Checkpoints are triggered by the `Controller` sending a `CheckpointRequest` to each source runtime via the control channel. Each source:

1. Snapshots its own state via `HandleCheckpoint`
2. Injects a `BarrierSignal` into each of its output buffers

The barrier travels **in-band** alongside data messages. When a node receives a barrier on all inputs (alignment), it:

1. Snapshots its state (`HandleCheckpoint`)
2. Forwards the barrier to all outputs
3. Reports `NodeCompletion` to the `Controller`

The `Controller` tracks completion across all nodes. When all nodes have reported, the checkpoint epoch is **complete**.

### Multi-input alignment (`BarrierAligner`)

When a node has multiple inputs (e.g., a merge node), barriers from different inputs may arrive at different times. The `BarrierAligner` buffers data from already-barriered inputs until all inputs have delivered the barrier for the same epoch — ensuring the snapshot is globally consistent.

```
Input A: data data [BARRIER] ← buffered
Input B: data        [BARRIER] ← triggers alignment
         └── snapshot state + forward barrier to output
```

### `ThenStop` flag

Passing `thenStop=true` to `InitiateCheckpoint` causes each node to exit after completing the checkpoint. This is how `Pipeline.Stop` achieves a graceful drain: data is processed, a final checkpoint is taken, then all nodes exit.

---

## Watermarks

Watermarks are `WatermarkSignal` messages injected by sources to express **event-time progress**: "all events with `EventTime < watermark.EventTime` have been emitted."

The `WatermarkHolder` at each node tracks the latest watermark from each input and emits an output watermark equal to the **minimum** across all non-idle inputs. This ensures merge nodes don't advance time until all upstream branches have caught up.

Operators receive watermarks via `HandleWatermark` and can use them to flush windows or trigger time-based computations.

---

## Pipeline lifecycle states

```
Initializing ──Start()──► Running ──Pause()──► Paused
                             │         └──Resume()──► Running
                             │
                             ├──Stop()──► Stopping ──► Stopped    (graceful)
                             └──Terminate()──► Terminated         (immediate)
```

- **Initializing**: `Build()` returned, `Start()` not yet called.
- **Running**: All goroutines active, data flowing.
- **Paused**: Goroutines alive but not reading from input buffers.
- **Stopping**: A `ThenStop` checkpoint barrier is propagating. Nodes exit after completing the final checkpoint.
- **Stopped**: All goroutines have exited after a graceful stop.
- **Terminated**: All goroutines exited after an immediate `Terminate()`.
- **Error**: A node returned a non-context error (only with `ErrorStrategyFailPipeline`).

---

## Error handling strategies

| Strategy | Behavior |
|---|---|
| `SkipAndLog` (default) | Log the error, skip the failed message, continue processing |
| `DeadLetter` | Route the failed message (wrapped in `DeadLetterMessage`) to a dead-letter sink |
| `FailPipeline` | Propagate the error; cancel the pipeline context; transition to `PipelineError` |

Errors from operators are captured by the `NodeRuntime` and dispatched to the configured `errorHandler`. Operators can also call `collector.EmitError(msg, err)` to explicitly report per-message errors without stopping the node.

---

## Graph validation

`Graph.Validate()` checks:
- At least one node exists
- No dangling nodes (every node has at least one edge)
- No incoming edges on source nodes
- No outgoing edges on sink nodes
- Operator nodes have both inputs and outputs
- No cycles (DFS with three-color marking)

---

## Concurrency model

Each node runs on exactly one goroutine (the `NodeRuntime.run` loop). Barriers, watermarks, and stop signals are serialized through the same input buffer as data, ensuring consistent ordering. The only exception is the out-of-band **control channel** (pause/resume/terminate), which bypasses the data flow.

For parallel data processing within a single node, wrap the operator with `nodes.Concurrent(op, n)` which dispatches messages to a worker pool of n goroutines.
