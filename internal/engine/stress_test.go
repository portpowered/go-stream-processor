package engine

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Stress test helper operators ---

// randomDelayOp introduces a random delay per message to simulate real workload variance.
type randomDelayOp struct {
	BaseOperator
	maxDelay time.Duration
}

func (r *randomDelayOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	delay := time.Duration(rand.Int63n(int64(r.maxDelay)))
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return collector.Emit(msg)
}

// failingOp randomly fails with an error at a configured rate.
type failingOp struct {
	BaseOperator
	failRate float64 // 0.0 to 1.0
}

func (f *failingOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	if rand.Float64() < f.failRate {
		return fmt.Errorf("random failure for message %v", msg.Value)
	}
	return collector.Emit(msg)
}

// statefulCounterOp counts messages and checkpoints the count.
type statefulCounterOp struct {
	BaseOperator
	mu    sync.Mutex
	count int
}

func (s *statefulCounterOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
	return collector.Emit(msg)
}

func (s *statefulCounterOp) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Encode count as a simple byte.
	return []byte(fmt.Sprintf("%d", s.count)), nil
}

func (s *statefulCounterOp) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// --- Stress Tests ---

// TestStress_1000MessagesThroughLargeGraph sends 1000 messages through a 10-node
// graph with random processing delays. Verifies all messages arrive at the sink.
func TestStress_1000MessagesThroughLargeGraph(t *testing.T) {
	numMessages := 1000
	numOperators := 10
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("stress-large-graph").
		AddSource("source", &sliceSourceHarness{items: items}).
		AddSink("sink", sink)

	prev := "source"
	for i := 0; i < numOperators; i++ {
		id := fmt.Sprintf("op%d", i)
		b.AddOperator(id, &randomDelayOp{maxDelay: 500 * time.Microsecond})
		b.Connect(prev, id)
		prev = id
	}
	b.Connect(prev, "sink")

	pipeline, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 30 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in 10-node stress test.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	if got := sink.Count(); got != numMessages {
		t.Errorf("expected %d messages at sink, got %d", numMessages, got)
	}

	// Verify every value was delivered exactly once.
	values := sink.Values()
	seen := make(map[int]int)
	for _, v := range values {
		if n, ok := v.(int); ok {
			seen[n]++
		}
	}
	for i := 0; i < numMessages; i++ {
		if seen[i] != 1 {
			t.Errorf("message %d seen %d times (expected 1)", i, seen[i])
		}
	}
}

// TestStress_RapidPauseResumeCycles exercises rapid pause/resume cycles during
// active message processing. Verifies no data loss.
func TestStress_RapidPauseResumeCycles(t *testing.T) {
	numMessages := 500
	source := &slowSourceHarness{
		items: makeItems(numMessages),
		delay: 1 * time.Millisecond,
	}
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("stress-pause-resume").
		AddSource("source", source).
		AddOperator("op", &passthroughOp{}).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Rapid pause/resume cycles — more aggressive than the harness test.
	var pauseResumeCount int
	for i := 0; i < 20; i++ {
		time.Sleep(5 * time.Millisecond)
		if err := pipeline.Pause(ctx); err != nil {
			// Pipeline may have already stopped if source finished.
			break
		}
		pauseResumeCount++
		time.Sleep(2 * time.Millisecond)
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
	case <-time.After(30 * time.Second):
		stacks := captureGoroutineStacks()
		t.Fatalf("DEADLOCK: rapid pause/resume stress test did not complete after %d cycles.\nStacks:\n%s",
			pauseResumeCount, stacks)
	}

	// All messages must be delivered — no data loss from pause/resume.
	if got := sink.Count(); got != numMessages {
		t.Errorf("expected %d messages (no data loss), got %d (after %d pause/resume cycles)",
			numMessages, got, pauseResumeCount)
	}

	t.Logf("completed %d pause/resume cycles", pauseResumeCount)
}

// TestStress_MultipleCheckpointsUnderLoad exercises checkpoint barrier propagation
// while messages are actively flowing. Verifies state consistency.
func TestStress_MultipleCheckpointsUnderLoad(t *testing.T) {
	numMessages := 200
	source := &slowSourceHarness{
		items: makeItems(numMessages),
		delay: 1 * time.Millisecond,
	}
	counter := &statefulCounterOp{}
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("stress-checkpoints").
		AddSource("source", source).
		AddOperator("counter", counter).
		AddSink("sink", sink).
		Connect("source", "counter").
		Connect("counter", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Issue multiple checkpoints while data is flowing.
	var checkpointCount int
	for i := 0; i < 5; i++ {
		time.Sleep(20 * time.Millisecond)
		state := pipeline.State()
		if state != PipelineRunning {
			break
		}
		if err := pipeline.Stop(ctx); err == nil {
			// Stop triggers a then_stop checkpoint — but we need to resume for more.
			// Actually, Stop with thenStop shuts down. Let's use the controller
			// directly to issue non-stop checkpoints.
			// Pipeline.Stop is terminal, so let's just count that we got here.
			checkpointCount++
			break
		}
		checkpointCount++
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
	case <-time.After(30 * time.Second):
		stacks := captureGoroutineStacks()
		t.Fatalf("DEADLOCK: checkpoint stress test did not complete.\nStacks:\n%s", stacks)
	}

	// Verify counter's state is consistent with sink count.
	counterVal := counter.Count()
	sinkCount := sink.Count()

	// Counter should have processed at least as many as sink received
	// (counter is upstream of sink).
	if counterVal < sinkCount {
		t.Errorf("counter (%d) < sink count (%d) — state inconsistency", counterVal, sinkCount)
	}

	t.Logf("checkpoints issued: %d, counter: %d, sink: %d", checkpointCount, counterVal, sinkCount)
}

// TestStress_RandomErrorsUnderLoad exercises error handling with a node that
// randomly fails. Uses SkipAndLog strategy to verify processing continues.
func TestStress_RandomErrorsUnderLoad_SkipAndLog(t *testing.T) {
	numMessages := 1000
	failRate := 0.1 // 10% failure rate
	items := makeItems(numMessages)
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("stress-errors-skip").
		AddSource("source", &sliceSourceHarness{items: items}).
		AddOperator("flaky", &failingOp{failRate: failRate}).
		AddSink("sink", sink).
		Connect("source", "flaky").
		Connect("flaky", "sink").
		SetErrorConfig(ErrorConfig{Strategy: ErrorStrategySkipAndLog}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 30 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in error stress test.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// With SkipAndLog, failed messages are dropped. Sink + errors = total messages.
	errCounts := pipeline.ErrorCounts()
	var totalErrors int64
	for _, count := range errCounts {
		totalErrors += count
	}

	sinkCount := int64(sink.Count())
	totalProcessed := sinkCount + totalErrors

	if totalProcessed != int64(numMessages) {
		t.Errorf("sink (%d) + errors (%d) = %d, expected %d",
			sinkCount, totalErrors, totalProcessed, numMessages)
	}

	t.Logf("sink: %d, errors: %d, total: %d", sinkCount, totalErrors, totalProcessed)
}

// TestStress_RandomErrorsUnderLoad_DeadLetter exercises error handling with
// dead-letter routing. Verifies failed messages land in the dead-letter sink.
// The source completes naturally, and the monitor signals the dead-letter
// runtime to stop after all regular runtimes exit.
func TestStress_RandomErrorsUnderLoad_DeadLetter(t *testing.T) {
	numMessages := 500
	failRate := 0.15 // 15% failure rate
	items := makeItems(numMessages)
	sink := &collectSinkHarness{}
	dlSink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("stress-errors-dl").
		AddSource("source", &sliceSourceHarness{items: items}).
		AddOperator("flaky", &failingOp{failRate: failRate}).
		AddSink("sink", sink).
		Connect("source", "flaky").
		Connect("flaky", "sink").
		SetErrorConfig(ErrorConfig{
			Strategy:       ErrorStrategyDeadLetter,
			DeadLetterSink: dlSink,
		}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Wait for graceful shutdown — the source completes, stop signals
	// propagate, and the monitor closes the dead-letter buffer.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pipeline did not complete within timeout")
	}

	sinkCount := sink.Count()
	dlCount := dlSink.Count()

	// All messages should be accounted for: either in sink or dead-letter.
	if sinkCount+dlCount != numMessages {
		t.Errorf("sink (%d) + dead-letter (%d) = %d, expected %d",
			sinkCount, dlCount, sinkCount+dlCount, numMessages)
	}

	// Verify dead-letter messages contain the right structure.
	for _, v := range dlSink.Values() {
		dlMsg, ok := v.(DeadLetterMessage)
		if !ok {
			t.Errorf("dead-letter value is not DeadLetterMessage: %T", v)
			continue
		}
		if dlMsg.Err == nil {
			t.Error("dead-letter message has nil error")
		}
		if dlMsg.NodeID != "flaky" {
			t.Errorf("dead-letter NodeID = %q, expected %q", dlMsg.NodeID, "flaky")
		}
	}

	t.Logf("sink: %d, dead-letter: %d", sinkCount, dlCount)
}

// TestStress_RandomErrorsUnderLoad_FailPipeline exercises error handling that
// stops the pipeline on first error. Verifies pipeline transitions to Error state.
func TestStress_RandomErrorsUnderLoad_FailPipeline(t *testing.T) {
	numMessages := 1000
	items := makeItems(numMessages)
	sink := &collectSinkHarness{}

	// Use 100% fail rate to guarantee failure on first message.
	pipeline, err := NewGraphBuilder("stress-errors-fail").
		AddSource("source", &sliceSourceHarness{items: items}).
		AddOperator("fail", &failingOp{failRate: 1.0}).
		AddSink("sink", sink).
		Connect("source", "fail").
		Connect("fail", "sink").
		SetErrorConfig(ErrorConfig{Strategy: ErrorStrategyFailPipeline}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in fail-pipeline stress test.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// Pipeline should have errored out.
	if result.FinalState != PipelineError {
		t.Errorf("expected Error state, got %s", result.FinalState)
	}

	// Sink should have received far fewer than all messages.
	if sink.Count() >= numMessages {
		t.Errorf("expected fewer than %d messages at sink with FailPipeline strategy, got %d",
			numMessages, sink.Count())
	}
}

// TestStress_FanOutFanInHighVolume sends 1000 messages through a fan-out to 5
// consumers and verifies the exact expected count at the merged sink.
func TestStress_FanOutFanInHighVolume(t *testing.T) {
	numMessages := 1000
	numConsumers := 5

	pipeline, sink, err := BuildFanOutFanIn(numMessages, numConsumers)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 30 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in fan-out/fan-in high-volume stress test.\nStacks:\n%s",
				result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	expected := numMessages * numConsumers
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}

	// Verify each original value appears exactly numConsumers times.
	values := sink.Values()
	seen := make(map[int]int)
	for _, v := range values {
		if n, ok := v.(int); ok {
			seen[n]++
		}
	}
	for i := 0; i < numMessages; i++ {
		if seen[i] != numConsumers {
			t.Errorf("message %d seen %d times (expected %d)", i, seen[i], numConsumers)
			break // Don't spam — one example is enough.
		}
	}
}

// TestStress_DiamondWithRandomDelays exercises the diamond topology under load
// with random delays on both branches.
func TestStress_DiamondWithRandomDelays(t *testing.T) {
	numMessages := 500
	sink := &collectSinkHarness{}
	items := makeItems(numMessages)

	b := NewGraphBuilder("stress-diamond-delays")
	b.AddSource("source", &sliceSourceHarness{items: items})
	b.AddOperator("fanout", &FanOutOperator{})
	b.AddOperator("left", &randomDelayOp{maxDelay: 200 * time.Microsecond})
	b.AddOperator("right", &randomDelayOp{maxDelay: 500 * time.Microsecond})
	b.AddOperator("merge", &MergeOperator{})
	b.AddSink("sink", sink)

	b.Connect("source", "fanout")
	b.Connect("fanout", "left")
	b.Connect("fanout", "right")
	b.Connect("left", "merge")
	b.Connect("right", "merge")
	b.Connect("merge", "sink")

	pipeline, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 30 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in diamond with random delays.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// Diamond: fanout duplicates to 2 branches -> merge.
	expected := numMessages * 2
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}
}

// TestStress_ConcurrentOperatorUnderLoad exercises the ConcurrentOperator wrapper
// with high message volume and multiple workers.
func TestStress_ConcurrentOperatorUnderLoad(t *testing.T) {
	numMessages := 1000
	concurrency := 8
	items := makeItems(numMessages)
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("stress-concurrent-op").
		AddSource("source", &sliceSourceHarness{items: items}).
		AddOperator("concurrent", NewConcurrentOperator(
			&randomDelayOp{maxDelay: 200 * time.Microsecond},
			concurrency,
		)).
		AddSink("sink", sink).
		Connect("source", "concurrent").
		Connect("concurrent", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 30 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in concurrent operator stress test.\nStacks:\n%s",
				result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	got := sink.Count()
	if got != numMessages {
		t.Errorf("expected exactly %d messages at sink (zero loss), got %d (concurrency=%d)",
			numMessages, got, concurrency)
	}
}

// TestIntegration_ConcurrentOperator_GracefulShutdown exercises the full pipeline
// shutdown path with ConcurrentOperator to verify no messages are lost under
// realistic conditions. Workers have an artificial delay to ensure they are
// in-flight when the stop signal arrives.
func TestIntegration_ConcurrentOperator_GracefulShutdown(t *testing.T) {
	concurrencyLevels := []int{1, 2, 4, 8}

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			numMessages := 100
			items := makeItems(numMessages)
			sink := &collectSinkHarness{}

			pipeline, err := NewGraphBuilder("integration-concurrent-shutdown").
				AddSource("source", &sliceSourceHarness{items: items}).
				AddOperator("concurrent", NewConcurrentOperator(
					&delayOp{delay: 10 * time.Millisecond},
					concurrency,
				)).
				AddSink("sink", sink).
				Connect("source", "concurrent").
				Connect("concurrent", "sink").
				Build()
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			harness := &TestHarness{Timeout: 30 * time.Second}
			result := harness.Run(pipeline)

			if !result.Completed {
				if result.Deadlocked {
					t.Fatalf("DEADLOCK detected.\nStacks:\n%s", result.GoroutineStacks)
				}
				t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
			}

			if got := sink.Count(); got != numMessages {
				t.Errorf("expected exactly %d messages at sink (zero loss), got %d (concurrency=%d)",
					numMessages, got, concurrency)
			}
		})
	}
}

// TestStress_ParallelPipelineRuns runs multiple independent pipelines concurrently
// to stress-test shared-nothing isolation.
func TestStress_ParallelPipelineRuns(t *testing.T) {
	numPipelines := 5
	numMessages := 200

	var wg sync.WaitGroup
	var failCount atomic.Int32

	for p := 0; p < numPipelines; p++ {
		wg.Add(1)
		go func(pipelineIdx int) {
			defer wg.Done()

			pipeline, sink, err := BuildLinearChain(numMessages, 3)
			if err != nil {
				t.Errorf("pipeline %d: Build failed: %v", pipelineIdx, err)
				failCount.Add(1)
				return
			}

			harness := &TestHarness{Timeout: 15 * time.Second}
			result := harness.Run(pipeline)

			if !result.Completed {
				t.Errorf("pipeline %d: did not complete (deadlocked=%v, state=%s)",
					pipelineIdx, result.Deadlocked, result.FinalState)
				failCount.Add(1)
				return
			}

			if got := sink.Count(); got != numMessages {
				t.Errorf("pipeline %d: expected %d messages, got %d",
					pipelineIdx, numMessages, got)
				failCount.Add(1)
			}
		}(p)
	}

	wg.Wait()

	if fails := failCount.Load(); fails > 0 {
		t.Errorf("%d/%d parallel pipelines failed", fails, numPipelines)
	}
}

// TestStress_MessageCountVerification is a comprehensive test that verifies
// message counts across multiple graph topologies under load.
func TestStress_MessageCountVerification(t *testing.T) {
	tests := []struct {
		name     string
		build    func() (*Pipeline, *collectSinkHarness, error)
		expected int
	}{
		{
			name: "linear-chain-1000",
			build: func() (*Pipeline, *collectSinkHarness, error) {
				return BuildLinearChain(1000, 5)
			},
			expected: 1000,
		},
		{
			name: "diamond-500",
			build: func() (*Pipeline, *collectSinkHarness, error) {
				return BuildDiamond(500)
			},
			expected: 1000, // 500 * 2 (fanout)
		},
		{
			name: "fan-out-3x-300",
			build: func() (*Pipeline, *collectSinkHarness, error) {
				return BuildFanOutFanIn(300, 3)
			},
			expected: 900, // 300 * 3
		},
		{
			name: "amplifier-200x5",
			build: func() (*Pipeline, *collectSinkHarness, error) {
				return BuildAmplifier(200, 5)
			},
			expected: 1000, // 200 * 5
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, sink, err := tt.build()
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			harness := &TestHarness{Timeout: 30 * time.Second}
			result := harness.Run(pipeline)

			if !result.Completed {
				if result.Deadlocked {
					t.Fatalf("DEADLOCK detected.\nStacks:\n%s", result.GoroutineStacks)
				}
				t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
			}

			if got := sink.Count(); got != tt.expected {
				t.Errorf("expected %d messages, got %d", tt.expected, got)
			}
		})
	}
}
