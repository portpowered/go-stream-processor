# go-stream-processor

An embedded stream processing library for Go, inspired by Apache Flink's architecture. Build fault-tolerant, stateful data pipelines directly inside your Go process — no external infrastructure required.

## Key features

- **DAG pipelines** – connect sources, operators, and sinks using a fluent builder API
- **Checkpointing** – barrier-based state snapshots with pluggable backends; end-to-end delivery guarantees depend on source replay and sink transaction behavior
- **Backpressure** – bounded buffers between nodes naturally rate-limit fast producers
- **Event-time watermarks** – reason about out-of-order data and trigger time-windowed operations
- **Lifecycle control** – pause, resume, graceful drain (`Stop`), or immediate shutdown (`Terminate`)
- **Functional constructors** – write operators as plain Go functions with no boilerplate

---

## Package layout

```
go-stream-processor/
├── stream_processor/   Primary user-facing API — Pipeline, GraphBuilder, Operator, Source, Collector
├── nodes/              Prebuilt operators — Map, Filter, FanOut, Merge, Router, Concurrent, SourceFunc, SinkFunc
└── internal/engine/    Runtime plumbing (buffers, barrier alignment, watermarks, controller) — not part of the public API
```

> **Import the `stream_processor` and `nodes` packages.** Do not import `internal/engine` directly.

## Development

Contributors should start with the [Embedded Stream Processor Development Guide](docs/development.md). It covers local commands, standalone verification, consumer adapter impact, and package-specific gotchas.

---

## Install

```bash
go get github.com/portpowered/go-stream-processor@v0.1.0
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/portpowered/go-stream-processor/nodes"
    sp "github.com/portpowered/go-stream-processor/stream_processor"
)

func main() {
    source := nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
        for _, v := range []int{1, 2, 3} {
            if err := emit("", v); err != nil { return err }
        }
        return nil
    })
    pipeline, err := sp.NewGraphBuilder("example").
        AddSource("source", source).
        AddOperator("double", nodes.Map(func(v int) int { return v * 2 })).
        AddSink("print", nodes.SinkFunc(func(v int) error { fmt.Println(v); return nil })).
        Connect("source", "double").Connect("double", "print").Build()
    if err != nil { log.Fatal(err) }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    pipeline.Start(ctx)
    if err := pipeline.Wait(); err != nil { log.Fatal(err) }
}
```

---

## Implementing a custom operator

For stateless transformations, use `nodes.Map` or `nodes.Filter`. For stateful or lifecycle-aware logic, implement the `Operator` interface and embed `BaseOperator` for default no-op methods:

```go
type MyCountOperator struct {
    sp.BaseOperator
    count int
}

func (o *MyCountOperator) ProcessMessage(ctx context.Context, msg sp.DataMessage, c sp.Collector) error {
    o.count++
    return c.Emit(sp.DataMessage{Key: msg.Key, Value: o.count})
}

// Optional: snapshot/restore for checkpoint support
func (o *MyCountOperator) HandleCheckpoint(ctx context.Context, b sp.BarrierSignal) ([]byte, error) {
    return []byte(fmt.Sprintf("%d", o.count)), nil
}
```

---

## Implementing a custom source

```go
type KafkaSource struct{ /* ... */ }

func (s *KafkaSource) Run(ctx context.Context, c sp.Collector) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        case msg := <-s.messages:
            if err := c.Emit(sp.DataMessage{Key: msg.Key, Value: msg.Value}); err != nil {
                return err
            }
        }
    }
}
```

Or with the functional constructor:

```go
src := nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        case v := <-ch:
            if err := emit("", v); err != nil {
                return err
            }
        }
    }
})
```

---

## Pipeline lifecycle

```
Initializing → Start() → Running → Pause() ⇆ Paused
                                 → Stop()   → Stopping → Stopped   (graceful drain)
                                 → Terminate() → Terminated        (immediate)
```

```go
pipeline.Start(ctx)

// Pause and resume processing
pipeline.Pause(ctx)
pipeline.Resume(ctx)

// Graceful shutdown: drain in-flight data, run a final checkpoint, then exit
pipeline.Stop(ctx)
pipeline.Wait()

// Immediate shutdown: exit without draining
pipeline.Terminate(ctx)
pipeline.Wait()
```

---

## Checkpointing

Connect a `CheckpointManager` to the controller to persist state snapshots:

```go
backend, _ := sp.NewLocalDiskBackend("/var/checkpoints/my-pipeline")
mgr := sp.NewCheckpointManager(sp.CheckpointManagerConfig{
    Backend:   backend,
    KeepCount: 3, // keep last 3 checkpoints
})
```

See [docs/architecture.md](docs/architecture.md) for how checkpoints flow through the graph.

---

## Error handling

Configure error handling when building the pipeline:

```go
pipeline, _ := sp.NewGraphBuilder("my-pipeline").
    SetErrorConfig(sp.ErrorConfig{
        Strategy: sp.ErrorStrategyDeadLetter,
        DeadLetterSink: myDeadLetterSink,
    }).
    // ... add nodes ...
    Build()
```

| Strategy | Behaviour |
|---|---|
| `ErrorStrategySkipAndLog` (default) | Skip failed message, log error, continue |
| `ErrorStrategyDeadLetter` | Route failed message to a dead-letter sink operator |
| `ErrorStrategyFailPipeline` | Propagate error to stop the pipeline |

---

## Testing

Use `TestHarness` to run a pipeline in tests with automatic timeout and deadlock detection:

```go
harness := &sp.TestHarness{Timeout: 5 * time.Second}
result  := harness.Run(pipeline)
if !result.Completed {
    t.Fatalf("pipeline did not complete: deadlocked=%v\n%s",
        result.Deadlocked, result.GoroutineStacks)
}
```

---

## Further reading

- [docs/architecture.md](docs/architecture.md) — internals, message model, barrier alignment, watermarks
- [docs/how-to-use.md](docs/how-to-use.md) — building operators, using fluent API, lifecycle states

## License

Apache-2.0; see [LICENSE](LICENSE).
