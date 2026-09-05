package engine

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

// TestHarness runs a pipeline with a configurable timeout and detects deadlocks
// by inspecting goroutine state when the pipeline fails to complete.
type TestHarness struct {
	// Timeout is the maximum duration to wait for the pipeline to complete.
	// If zero, defaults to 10 seconds.
	Timeout time.Duration
}

// TestHarnessResult contains the outcome of a test harness run.
type TestHarnessResult struct {
	// Completed is true if the pipeline finished within the timeout.
	Completed bool
	// Deadlocked is true if goroutines were blocked when the timeout expired.
	Deadlocked bool
	// Duration is how long the pipeline ran before completing or timing out.
	Duration time.Duration
	// Error is any error from the pipeline.
	Error error
	// GoroutineStacks contains goroutine stack traces if a deadlock was detected.
	GoroutineStacks string
	// FinalState is the pipeline's state when the harness finished.
	FinalState PipelineState
}

// Run starts the pipeline and waits for it to complete or timeout.
// If the pipeline doesn't complete within the timeout and goroutines appear
// blocked, it reports a deadlock with goroutine stack traces.
func (h *TestHarness) Run(pipeline *Pipeline) TestHarnessResult {
	timeout := h.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	pipeline.Start(ctx)

	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		return TestHarnessResult{
			Completed:  true,
			Duration:   time.Since(start),
			FinalState: pipeline.State(),
		}
	case <-time.After(timeout):
		// Pipeline didn't complete — check for deadlock.
		stacks := captureGoroutineStacks()
		deadlocked := hasBlockedGoroutines(stacks)

		// Force shutdown so goroutines don't leak.
		cancel()

		// Give a brief moment for cleanup.
		cleanupDone := make(chan struct{})
		go func() {
			pipeline.Wait()
			close(cleanupDone)
		}()
		select {
		case <-cleanupDone:
		case <-time.After(2 * time.Second):
		}

		return TestHarnessResult{
			Completed:       false,
			Deadlocked:      deadlocked,
			Duration:        time.Since(start),
			FinalState:      pipeline.State(),
			GoroutineStacks: stacks,
		}
	}
}

// captureGoroutineStacks returns all goroutine stack traces.
func captureGoroutineStacks() string {
	buf := make([]byte, 1<<20) // 1MB
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

// hasBlockedGoroutines checks if the goroutine stacks indicate blocked goroutines
// (chan send, chan receive, select, semacquire) that suggest a deadlock.
func hasBlockedGoroutines(stacks string) bool {
	blockIndicators := []string{
		"chan send",
		"chan receive",
		"select",
		"semacquire",
	}
	// Look for esp package goroutines that are blocked.
	sections := strings.Split(stacks, "\n\n")
	for _, section := range sections {
		if !strings.Contains(section, "go-stream-processor") {
			continue
		}
		for _, indicator := range blockIndicators {
			if strings.Contains(section, indicator) {
				return true
			}
		}
	}
	return false
}

// --- Built-in test graph factories ---

// BuildLinearChain builds a linear pipeline: source -> op1 -> op2 -> ... -> sink.
// The operator is a passthrough. numOperators is the number of intermediate nodes.
func BuildLinearChain(numMessages int, numOperators int) (*Pipeline, *collectSinkHarness, error) {
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("linear-chain").
		AddSource("source", &sliceSourceHarness{items: items}).
		AddSink("sink", sink)

	prev := "source"
	for i := 0; i < numOperators; i++ {
		id := fmt.Sprintf("op%d", i)
		b.AddOperator(id, &passthroughOp{})
		b.Connect(prev, id)
		prev = id
	}
	b.Connect(prev, "sink")

	p, err := b.Build()
	return p, sink, err
}

// BuildDiamond builds a diamond graph: source -> A, source -> B, A -> sink, B -> sink.
// Both A and B are passthrough operators.
func BuildDiamond(numMessages int) (*Pipeline, *collectSinkHarness, error) {
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("diamond")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("fanout", &FanOutOperator{})
	b.AddOperator("left", &passthroughOp{})
	b.AddOperator("right", &passthroughOp{})
	b.AddOperator("merge", &MergeOperator{})
	b.AddSink("sink", sink)

	b.Connect("source", "fanout")
	b.Connect("fanout", "left")
	b.Connect("fanout", "right")
	b.Connect("left", "merge")
	b.Connect("right", "merge")
	b.Connect("merge", "sink")

	p, err := b.Build()
	return p, sink, err
}

// BuildFanOutFanIn builds a fan-out/fan-in graph:
// source -> fanout -> [consumer0..consumerN-1] -> merge -> sink.
func BuildFanOutFanIn(numMessages int, numConsumers int) (*Pipeline, *collectSinkHarness, error) {
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("fan-out-fan-in")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("fanout", &FanOutOperator{})
	b.AddOperator("merge", &MergeOperator{})
	b.AddSink("sink", sink)

	b.Connect("source", "fanout")
	for i := 0; i < numConsumers; i++ {
		id := fmt.Sprintf("consumer%d", i)
		b.AddOperator(id, &passthroughOp{})
		b.Connect("fanout", id)
		b.Connect(id, "merge")
	}
	b.Connect("merge", "sink")

	p, err := b.Build()
	return p, sink, err
}

// BuildSlowConsumerDiamond builds a diamond where one branch has a slow consumer.
func BuildSlowConsumerDiamond(numMessages int, slowDelay time.Duration) (*Pipeline, *collectSinkHarness, error) {
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("slow-consumer-diamond")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("fanout", &FanOutOperator{})
	b.AddOperator("fast", &passthroughOp{})
	b.AddOperator("slow", &delayOp{delay: slowDelay})
	b.AddOperator("merge", &MergeOperator{})
	b.AddSink("sink", sink)

	b.Connect("source", "fanout")
	b.Connect("fanout", "fast")
	b.Connect("fanout", "slow")
	b.Connect("fast", "merge")
	b.Connect("slow", "merge")
	b.Connect("merge", "sink")

	p, err := b.Build()
	return p, sink, err
}

// BuildAmplifier builds a graph with a node that produces N outputs per 1 input.
func BuildAmplifier(numMessages int, amplification int) (*Pipeline, *collectSinkHarness, error) {
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("amplifier")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("amplify", &amplifyOp{factor: amplification})
	b.AddSink("sink", sink)
	b.Connect("source", "amplify")
	b.Connect("amplify", "sink")

	p, err := b.Build()
	return p, sink, err
}

// --- Test helper types for harness ---

// collectSinkHarness stores all received message values (thread-safe).
type collectSinkHarness struct {
	BaseOperator
	mu     sync.Mutex
	values []interface{}
}

func (s *collectSinkHarness) ProcessMessage(_ context.Context, msg DataMessage, _ Collector) error {
	s.mu.Lock()
	s.values = append(s.values, msg.Value)
	s.mu.Unlock()
	return nil
}

func (s *collectSinkHarness) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.values)
}

func (s *collectSinkHarness) Values() []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]interface{}, len(s.values))
	copy(result, s.values)
	return result
}

// sliceSourceHarness emits a fixed set of values and returns.
type sliceSourceHarness struct {
	items []interface{}
}

func (s *sliceSourceHarness) Run(ctx context.Context, collector Collector) error {
	for _, item := range s.items {
		if err := collector.Emit(DataMessage{Value: item}); err != nil {
			return err
		}
	}
	return nil
}

// passthroughOp is a no-op operator that forwards messages unchanged.
type passthroughOp struct {
	BaseOperator
}

func (p *passthroughOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	return collector.Emit(msg)
}

// delayOp introduces a delay per message to simulate slow processing.
type delayOp struct {
	BaseOperator
	delay time.Duration
}

func (d *delayOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return collector.Emit(msg)
}

// amplifyOp produces `factor` output messages for each input message.
type amplifyOp struct {
	BaseOperator
	factor int
}

func (a *amplifyOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	for i := 0; i < a.factor; i++ {
		if err := collector.Emit(DataMessage{
			Key:       msg.Key,
			Value:     msg.Value,
			EventTime: msg.EventTime,
		}); err != nil {
			return err
		}
	}
	return nil
}

// makeItems creates a slice of integer items [0, n).
func makeItems(n int) []interface{} {
	items := make([]interface{}, n)
	for i := range items {
		items[i] = i
	}
	return items
}
