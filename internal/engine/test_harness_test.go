package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- Deadlock detection tests ---

func TestHarness_DiamondWithSlowConsumerNoDeadlock(t *testing.T) {
	pipeline, sink, err := BuildSlowConsumerDiamond(20, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in diamond with slow consumer.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// Each message goes through fanout (duplicated to 2 branches), then merged.
	// Expected: numMessages * 2
	expected := 20 * 2
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}
}

func TestHarness_FanOutTo10ConsumersNoDeadlock(t *testing.T) {
	pipeline, sink, err := BuildFanOutFanIn(50, 10)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in fan-out to 10 consumers.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// Each message goes through fanout (duplicated to 10 consumers), then merged.
	expected := 50 * 10
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}
}

func TestHarness_AmplifierNoDeadlock(t *testing.T) {
	pipeline, sink, err := BuildAmplifier(20, 5)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected with amplifier node.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	expected := 20 * 5
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}
}

func TestHarness_PauseResumeDuringHeavyLoadNoDeadlock(t *testing.T) {
	// Use a slow source that produces many messages, allowing us to pause/resume mid-stream.
	source := &slowSourceHarness{
		items: makeItems(100),
		delay: 2 * time.Millisecond,
	}
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("pause-resume-load").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Rapid pause/resume cycles while messages are flowing.
	for i := 0; i < 5; i++ {
		time.Sleep(10 * time.Millisecond)
		if err := pipeline.Pause(ctx); err != nil {
			// Pipeline may have already stopped if source finished.
			break
		}
		time.Sleep(5 * time.Millisecond)
		if err := pipeline.Resume(ctx); err != nil {
			break
		}
	}

	// Wait for natural completion with timeout.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(10 * time.Second):
		stacks := captureGoroutineStacks()
		t.Fatalf("DEADLOCK: pause/resume under load did not complete.\nStacks:\n%s", stacks)
	}

	// All 100 messages should have been delivered (no data loss from pause/resume).
	if got := sink.Count(); got != 100 {
		t.Errorf("expected 100 messages (no data loss), got %d", got)
	}
}

func TestHarness_LinearChainNoDeadlock(t *testing.T) {
	pipeline, sink, err := BuildLinearChain(100, 5)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in linear chain.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	if got := sink.Count(); got != 100 {
		t.Errorf("expected 100 messages at sink, got %d", got)
	}
}

func TestHarness_FanOutWithVaryingSpeedsNoDeadlock(t *testing.T) {
	// Fan-out to consumers with different processing speeds.
	sink := &collectSinkHarness{}
	items := makeItems(30)

	b := NewGraphBuilder("varying-speeds")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("fanout", &FanOutOperator{})
	b.AddOperator("fast", &passthroughOp{})
	b.AddOperator("medium", &delayOp{delay: 2 * time.Millisecond})
	b.AddOperator("slow", &delayOp{delay: 5 * time.Millisecond})
	b.AddOperator("merge", &MergeOperator{})
	b.AddSink("sink", sink)

	b.Connect("source", "fanout")
	b.Connect("fanout", "fast")
	b.Connect("fanout", "medium")
	b.Connect("fanout", "slow")
	b.Connect("fast", "merge")
	b.Connect("medium", "merge")
	b.Connect("slow", "merge")
	b.Connect("merge", "sink")

	pipeline, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected with varying speed consumers.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// 30 messages × 3 consumers = 90
	expected := 30 * 3
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}
}

func TestHarness_DiamondGraphCompletesCorrectly(t *testing.T) {
	pipeline, sink, err := BuildDiamond(50)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in diamond graph.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// Diamond: source -> fanout -> left/right -> merge -> sink
	// Each message duplicated by fanout = 50 * 2 = 100
	expected := 50 * 2
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}

	if result.FinalState != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", result.FinalState)
	}
}

func TestHarness_TimeoutReportsDeadlockInfo(t *testing.T) {
	// Verify that when a pipeline times out, the harness reports useful info.
	// Use a blocking source that never completes.
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("timeout-test").
		AddSource("source", &blockingSourceHarness{}).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 500 * time.Millisecond}
	result := harness.Run(pipeline)

	if result.Completed {
		t.Fatal("expected pipeline to timeout, but it completed")
	}

	// The harness should capture goroutine stacks.
	if result.GoroutineStacks == "" {
		t.Error("expected goroutine stacks in result, got empty string")
	}

	t.Logf("timeout after %v, deadlocked=%v, state=%s", result.Duration, result.Deadlocked, result.FinalState)
}

func TestHarness_FanOutFanInWithSmallBuffers(t *testing.T) {
	// Small buffers increase deadlock risk — verify it works.
	sink := &collectSinkHarness{}
	items := makeItems(20)

	b := NewGraphBuilder("small-buffers")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("fanout", &FanOutOperator{})
	b.AddOperator("merge", &MergeOperator{})
	b.AddSink("sink", sink)

	b.Connect("source", "fanout")
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("worker%d", i)
		b.AddOperator(id, &passthroughOp{})
		b.Connect("fanout", id)
		b.Connect(id, "merge")
		b.SetBufferSize("fanout", id, 2)
		b.SetBufferSize(id, "merge", 2)
	}
	b.Connect("merge", "sink")
	b.SetBufferSize("source", "fanout", 2)
	b.SetBufferSize("merge", "sink", 2)

	pipeline, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected with small buffers.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	expected := 20 * 5
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}
}

// --- Additional test helpers ---

// slowSourceHarness emits items with a delay between each.
type slowSourceHarness struct {
	items []interface{}
	delay time.Duration
}

func (s *slowSourceHarness) Run(ctx context.Context, collector Collector) error {
	for _, item := range s.items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.delay):
		}
		if err := collector.Emit(DataMessage{Value: item}); err != nil {
			return err
		}
	}
	return nil
}

// blockingSourceHarness blocks until context is cancelled.
type blockingSourceHarness struct{}

func (s *blockingSourceHarness) Run(ctx context.Context, _ Collector) error {
	<-ctx.Done()
	return ctx.Err()
}

// concurrentCountSink counts received messages thread-safely and tracks per-value counts.
type concurrentCountSink struct {
	BaseOperator
	mu     sync.Mutex
	count  int
	values map[interface{}]int
}

func newConcurrentCountSink() *concurrentCountSink {
	return &concurrentCountSink{values: make(map[interface{}]int)}
}

func (s *concurrentCountSink) ProcessMessage(_ context.Context, msg DataMessage, _ Collector) error {
	s.mu.Lock()
	s.count++
	s.values[msg.Value]++
	s.mu.Unlock()
	return nil
}

func (s *concurrentCountSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
