# How to use go-stream-processor

This guide explains how to build operators, wire pipelines, handle lifecycle states, and use advanced features like timers and checkpoints.

---

## Building operators

### Option 1 — Functional constructors (recommended for simple cases)

The `nodes` package provides zero-boilerplate constructors for the most common patterns:

```go
import "github.com/portpowered/go-stream-processor/nodes"

// Transform each value
double := nodes.Map(func(v int) int { return v * 2 })

// Keep only matching values
evens := nodes.Filter(func(v int) bool { return v%2 == 0 })

// Create a source from a function
src := nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
    for _, item := range data {
        if err := emit("key", item); err != nil {
            return err
        }
    }
    return nil
})

// Create a sink from a typed function
printer := nodes.SinkFunc(func(v string) error {
    fmt.Println(v)
    return nil
})
```

### Option 2 — Implementing the Operator interface

For stateful or lifecycle-aware operators, implement `Operator` and embed `BaseOperator` for default no-op methods:

```go
import sp "github.com/portpowered/go-stream-processor/stream_processor"

type RunningAverageOp struct {
    sp.BaseOperator
    sum   float64
    count int
}

func (o *RunningAverageOp) ProcessMessage(
    ctx context.Context,
    msg sp.DataMessage,
    c sp.Collector,
) error {
    v, ok := msg.Value.(float64)
    if !ok {
        return nil // skip wrong-type messages
    }
    o.sum += v
    o.count++
    avg := o.sum / float64(o.count)
    return c.Emit(sp.DataMessage{Key: msg.Key, Value: avg, EventTime: msg.EventTime})
}
```

The `BaseOperator` provides default no-ops for:
- `HandleWatermark` — called when event-time advances
- `HandleCheckpoint` — called to snapshot state (return `nil, nil` if stateless)
- `OnStart` — called once when the node starts
- `OnClose` — called once when the node stops

Override only the methods you need.

### Operator methods reference

| Method | When called | Typical use |
|---|---|---|
| `ProcessMessage(ctx, msg, collector)` | For each data message | Transform, filter, aggregate |
| `HandleWatermark(ctx, wm, collector)` | When event-time watermark advances | Flush time windows |
| `HandleCheckpoint(ctx, barrier)` | On checkpoint barrier | Serialize state to `[]byte` |
| `OnStart(ctx)` | Before first message | Open connections, initialize state |
| `OnClose(ctx)` | After last message | Flush buffers, close connections |

---

## Using the Collector

`Collector` is passed into every operator method. Use it to emit results and register timers.

```go
// Emit to the default output
c.Emit(sp.DataMessage{Key: "k", Value: result})

// Emit to a named output port (for fan-out / routing)
c.EmitToPort("fast-path", sp.DataMessage{Key: "k", Value: result})

// Report a per-message error without stopping the node
c.EmitError(msg, fmt.Errorf("failed to parse: %w", err))

// Register an event-time timer (operator must implement TimerOperator)
c.RegisterTimer("flush-window", time.Now().Add(10*time.Second))
```

---

## Implementing a Source

Sources produce data via a long-running `Run` loop. Return when the source is exhausted or `ctx` is cancelled:

```go
type TickerSource struct {
    Interval time.Duration
}

func (s *TickerSource) Run(ctx context.Context, c sp.Collector) error {
    ticker := time.NewTicker(s.Interval)
    defer ticker.Stop()
    seq := 0
    for {
        select {
        case <-ctx.Done():
            return nil
        case t := <-ticker.C:
            seq++
            err := c.Emit(sp.DataMessage{
                Key:       fmt.Sprintf("tick-%d", seq),
                Value:     seq,
                EventTime: t,
            })
            if err != nil {
                return err
            }
        }
    }
}
```

---

## Using the fluent GraphBuilder

```go
pipeline, err := sp.NewGraphBuilder("my-pipeline").
    // Add nodes
    AddSource("source", mySource).
    AddOperator("transform", myOp).
    AddSink("sink", mySink).
    // Connect them (simple default ports)
    Connect("source", "transform").
    Connect("transform", "sink").
    // Optional: named port connections for routing
    ConnectPort("router", "positive", "pos-sink", "default").
    ConnectPort("router", "negative", "neg-sink", "default").
    // Optional: override buffer sizes between specific nodes
    SetBufferSize("source", "transform", 256).
    // Optional: configure error handling
    SetErrorConfig(sp.ErrorConfig{Strategy: sp.ErrorStrategySkipAndLog}).
    Build()
```

`Build()` validates the graph (no cycles, no dangling nodes, proper source/sink constraints) and returns an executable `*Pipeline`.

---

## Building routing topologies

### Fan-out (broadcast to multiple branches)

```go
b.AddOperator("fanout", nodes.FanOut()).
    Connect("source", "fanout").
    Connect("fanout", "branch-a").
    Connect("fanout", "branch-b")
```

### Merge (join multiple streams)

```go
b.AddOperator("merge", nodes.Merge()).
    Connect("branch-a", "merge").
    Connect("branch-b", "merge").
    Connect("merge", "sink")
```

### Conditional routing

```go
b.AddOperator("router", nodes.Router(func(msg sp.DataMessage) string {
    if msg.Value.(int) > 100 {
        return "high"
    }
    return "low"
}, "low")). // "low" is the default port for unmatched messages
    Connect("source", "router").
    ConnectPort("router", "high", "high-sink", "default").
    ConnectPort("router", "low",  "low-sink",  "default")
```

---

## Pipeline lifecycle states

```
Build()  →  Initializing
Start()  →  Running
Pause()  →  Paused       (goroutines alive, not processing)
Resume() →  Running
Stop()   →  Stopping → Stopped      (graceful drain + final checkpoint)
Terminate() → Terminated            (immediate exit, no final checkpoint)
```

```go
ctx := context.Background()
pipeline.Start(ctx)

// Check state
state := pipeline.State() // PipelineRunning, PipelinePaused, etc.
fmt.Println(state) // "Running"

// Graceful shutdown — waits for in-flight data + runs a final checkpoint
if err := pipeline.Stop(ctx); err != nil {
    log.Fatal(err)
}
pipeline.Wait() // blocks until all goroutines have exited

// Immediate shutdown — no drain, no final checkpoint
pipeline.Terminate(ctx)
pipeline.Wait()
```

### Error counts

```go
counts := pipeline.ErrorCounts() // map[nodeID]int64
for nodeID, n := range counts {
    log.Printf("node %s had %d errors", nodeID, n)
}
```

---

## Stateful operators and checkpointing

Implement `HandleCheckpoint` to persist state:

```go
func (o *CounterOp) HandleCheckpoint(_ context.Context, _ sp.BarrierSignal) ([]byte, error) {
    return []byte(strconv.Itoa(o.count)), nil
}
```

To restore from a checkpoint, load the state bytes in `OnStart` (you must wire checkpoint loading yourself via `CheckpointManager`).

For complex keyed state, use `sp.InMemoryStateStore`:

```go
type StatefulOp struct {
    sp.BaseOperator
    store *sp.InMemoryStateStore
}

func NewStatefulOp() *StatefulOp {
    return &StatefulOp{store: sp.NewInMemoryStateStore()}
}

func (o *StatefulOp) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
    // Read existing state for this key
    existing := o.store.Get(msg.Key)
    // ... compute new state ...
    o.store.Put(msg.Key, newState)
    return c.Emit(msg)
}

func (o *StatefulOp) HandleCheckpoint(_ context.Context, _ sp.BarrierSignal) ([]byte, error) {
    return o.store.Snapshot(), nil
}
```

---

## Timer operators

To schedule event-time callbacks, implement `TimerOperator` (which extends `Operator`):

```go
type DelayOp struct {
    sp.BaseOperator
    Delay time.Duration
    pending map[string]sp.DataMessage
}

func (o *DelayOp) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
    id := uuid.New().String()
    o.pending[id] = msg
    c.RegisterTimer(id, time.Now().Add(o.Delay))
    return nil
}

func (o *DelayOp) HandleTimer(_ context.Context, name string, _ time.Time, c sp.Collector) error {
    if msg, ok := o.pending[name]; ok {
        delete(o.pending, name)
        return c.Emit(msg)
    }
    return nil
}

func (o *DelayOp) HandleCheckpoint(_ context.Context, _ sp.BarrierSignal) ([]byte, error) {
    // Timer registrations are automatically checkpointed by the runtime.
    // Only snapshot your own state (e.g., pending messages) here.
    return nil, nil
}
```

Prebuilt timer operators: `sp.NewSleepOperator(delay)`, `sp.NewDebounceOperator(window)`, `sp.NewDeduplicateOperator(window)`.

---

## Periodic tick operators

For operators that need a periodic background callback (e.g., to flush a buffer every N seconds), implement `TickOperator`:

```go
type FlushingOp struct {
    sp.BaseOperator
    mu     sync.Mutex
    buffer []sp.DataMessage
}

func (o *FlushingOp) TickInterval() time.Duration { return 5 * time.Second }

func (o *FlushingOp) HandleTick(_ context.Context, c sp.Collector) error {
    o.mu.Lock()
    batch := o.buffer
    o.buffer = nil
    o.mu.Unlock()
    for _, msg := range batch {
        if err := c.Emit(msg); err != nil {
            return err
        }
    }
    return nil
}

func (o *FlushingOp) ProcessMessage(_ context.Context, msg sp.DataMessage, _ sp.Collector) error {
    o.mu.Lock()
    o.buffer = append(o.buffer, msg)
    o.mu.Unlock()
    return nil
}
```

---

## Testing pipelines

Use `TestHarness` in tests:

```go
func TestMyPipeline(t *testing.T) {
    pipeline, err := buildMyPipeline()
    if err != nil {
        t.Fatal(err)
    }

    harness := &sp.TestHarness{Timeout: 5 * time.Second}
    result := harness.Run(pipeline)

    if !result.Completed {
        t.Fatalf("pipeline did not complete (deadlocked=%v):\n%s",
            result.Deadlocked, result.GoroutineStacks)
    }
    // Assert on collected output...
}
```

---

## Parallel processing within a node

Wrap any operator with `nodes.Concurrent` to process messages on a goroutine pool:

```go
heavyOp := nodes.Concurrent(myExpensiveOp, 8) // 8 parallel workers
```

Output ordering is **not guaranteed** when using concurrent processing. Checkpoint barriers wait for all in-flight workers to complete before proceeding, ensuring consistent snapshots.

---

## Watermarks and event-time processing

Emit watermarks from your source to advance event time in downstream operators:

```go
func (s *MySource) Run(ctx context.Context, c sp.Collector) error {
    for _, event := range events {
        c.Emit(sp.DataMessage{Key: event.Key, Value: event, EventTime: event.Timestamp})
        // Periodically advance the watermark
        c.EmitWatermark(event.Timestamp) // Note: use sp.WatermarkSignal via raw Node interface
    }
    return nil
}
```

Operators receive watermark notifications via `HandleWatermark`. The runtime tracks the minimum watermark across all inputs for merge nodes, so downstream operators only see a watermark advance when all upstream branches have caught up.
