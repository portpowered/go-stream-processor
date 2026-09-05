package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Quiescence detection tests (US-020) ---

// TestQuiescence_SingleSourceSingleSink verifies quiescence is reached after
// a single source completes and all messages are drained through the sink.
func TestQuiescence_SingleSourceSingleSink(t *testing.T) {
	numMessages := 50
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("quiescence-single").
		AddSource("source", &sliceSourceHarness{items: makeItems(numMessages)}).
		AddOperator("op", &passthroughOp{}).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	// Pipeline should reach Stopped state (quiescent).
	if result.FinalState != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", result.FinalState)
	}

	if got := sink.Count(); got != numMessages {
		t.Errorf("expected %d messages at sink, got %d", numMessages, got)
	}
}

// channelSource is a source that reads from a channel, allowing external control
// of when and what data is produced.
type channelSource struct {
	ch <-chan interface{}
}

func (s *channelSource) Run(ctx context.Context, collector Collector) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-s.ch:
			if !ok {
				return nil // channel closed = source done
			}
			if err := collector.Emit(DataMessage{Value: item}); err != nil {
				return err
			}
		}
	}
}

// TestQuiescence_MultipleSourcesSingleSink verifies quiescence is only reached
// after ALL sources complete — not just the first one.
func TestQuiescence_MultipleSourcesSingleSink(t *testing.T) {
	numPerSource := 25

	// Two independently-controlled sources.
	ch1 := make(chan interface{}, numPerSource)
	ch2 := make(chan interface{}, numPerSource)

	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("quiescence-multi-source").
		AddSource("source1", &channelSource{ch: ch1}).
		AddSource("source2", &channelSource{ch: ch2}).
		AddOperator("merge", &MergeOperator{}).
		AddSink("sink", sink).
		Connect("source1", "merge").
		Connect("source2", "merge").
		Connect("merge", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Source1 emits all its messages and closes.
	for i := 0; i < numPerSource; i++ {
		ch1 <- i
	}
	close(ch1)

	// Give time for source1's messages to propagate.
	time.Sleep(100 * time.Millisecond)

	// Pipeline should NOT be stopped yet — source2 is still active.
	state := pipeline.State()
	if state == PipelineStopped {
		t.Fatal("pipeline should not be stopped while source2 is still active")
	}

	// Source2 emits its messages and closes.
	for i := numPerSource; i < numPerSource*2; i++ {
		ch2 <- i
	}
	close(ch2)

	// Wait for pipeline to complete.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		stacks := captureGoroutineStacks()
		t.Fatalf("DEADLOCK: multi-source quiescence did not complete.\nStacks:\n%s", stacks)
	}

	if pipeline.State() != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", pipeline.State())
	}

	// All messages from both sources should arrive.
	totalExpected := numPerSource * 2
	if got := sink.Count(); got != totalExpected {
		t.Errorf("expected %d messages at sink, got %d", totalExpected, got)
	}
}

// emptySource is a source that immediately returns without emitting any messages.
type emptySource struct{}

func (s *emptySource) Run(ctx context.Context, collector Collector) error {
	return nil
}

// TestQuiescence_ZeroMessageSource verifies quiescence is reached even when
// a source generates 0 messages.
func TestQuiescence_ZeroMessageSource(t *testing.T) {
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("quiescence-zero-msg").
		AddSource("source", &emptySource{}).
		AddOperator("op", &passthroughOp{}).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected with zero-message source.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	if result.FinalState != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", result.FinalState)
	}

	// Sink should receive 0 messages.
	if got := sink.Count(); got != 0 {
		t.Errorf("expected 0 messages at sink, got %d", got)
	}
}

// TestQuiescence_AmplificationNode verifies quiescence accounts for message
// amplification — a node that generates N outputs per input.
func TestQuiescence_AmplificationNode(t *testing.T) {
	numMessages := 30
	amplification := 5
	sink := &collectSinkHarness{}

	pipeline, err := NewGraphBuilder("quiescence-amplify").
		AddSource("source", &sliceSourceHarness{items: makeItems(numMessages)}).
		AddOperator("amplify", &amplifyOp{factor: amplification}).
		AddSink("sink", sink).
		Connect("source", "amplify").
		Connect("amplify", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected with amplification.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	if result.FinalState != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", result.FinalState)
	}

	// Verify amplified message count.
	expected := numMessages * amplification
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink (amplification=%d), got %d",
			expected, amplification, got)
	}

	// Verify each original value appears exactly `amplification` times.
	values := sink.Values()
	seen := make(map[int]int)
	for _, v := range values {
		if n, ok := v.(int); ok {
			seen[n]++
		}
	}
	for i := 0; i < numMessages; i++ {
		if seen[i] != amplification {
			t.Errorf("message %d seen %d times (expected %d)", i, seen[i], amplification)
			break
		}
	}
}

// TestQuiescence_DiamondGraph verifies quiescence handles message duplication
// via fan-out in a diamond topology.
func TestQuiescence_DiamondGraph(t *testing.T) {
	numMessages := 40

	pipeline, sink, err := BuildDiamond(numMessages)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	harness := &TestHarness{Timeout: 10 * time.Second}
	result := harness.Run(pipeline)

	if !result.Completed {
		if result.Deadlocked {
			t.Fatalf("DEADLOCK detected in diamond quiescence.\nStacks:\n%s", result.GoroutineStacks)
		}
		t.Fatalf("pipeline did not complete within timeout (state: %s)", result.FinalState)
	}

	if result.FinalState != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", result.FinalState)
	}

	// Diamond: each message duplicated by fanout → 2 branches → merge.
	expected := numMessages * 2
	if got := sink.Count(); got != expected {
		t.Errorf("expected %d messages at sink, got %d", expected, got)
	}

	// Verify each original value appears exactly 2 times.
	values := sink.Values()
	seen := make(map[int]int)
	for _, v := range values {
		if n, ok := v.(int); ok {
			seen[n]++
		}
	}
	for i := 0; i < numMessages; i++ {
		if seen[i] != 2 {
			t.Errorf("message %d seen %d times (expected 2)", i, seen[i])
			break
		}
	}
}

// checkpointSnapshotSink stores received messages and supports checkpointing
// the count as state, allowing verification of state consistency at quiescence.
type checkpointSnapshotSink struct {
	BaseOperator
	mu     sync.Mutex
	values []interface{}
	count  int
}

func (s *checkpointSnapshotSink) ProcessMessage(_ context.Context, msg DataMessage, _ Collector) error {
	s.mu.Lock()
	s.values = append(s.values, msg.Value)
	s.count++
	s.mu.Unlock()
	return nil
}

func (s *checkpointSnapshotSink) HandleCheckpoint(_ context.Context, _ BarrierSignal) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []byte(fmt.Sprintf("%d", s.count)), nil
}

func (s *checkpointSnapshotSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *checkpointSnapshotSink) Values() []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]interface{}, len(s.values))
	copy(result, s.values)
	return result
}

// TestQuiescence_CheckpointedStateConsistency verifies that quiescence with
// checkpointed state is consistent — the state snapshot contents match
// the actual data processed.
func TestQuiescence_CheckpointedStateConsistency(t *testing.T) {
	numMessages := 100
	counter := &statefulCounterOp{}
	sink := &checkpointSnapshotSink{}

	pipeline, err := NewGraphBuilder("quiescence-checkpoint-state").
		AddSource("source", &sliceSourceHarness{items: makeItems(numMessages)}).
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

	// Let data flow for a bit, then stop with a final checkpoint.
	time.Sleep(100 * time.Millisecond)

	// Stop triggers a then_stop checkpoint barrier.
	if err := pipeline.Stop(ctx); err != nil {
		// Pipeline may have already stopped if source completed quickly.
		if pipeline.State() != PipelineStopped && pipeline.State() != PipelineStopping {
			t.Fatalf("Stop failed: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		stacks := captureGoroutineStacks()
		t.Fatalf("DEADLOCK: checkpointed quiescence did not complete.\nStacks:\n%s", stacks)
	}

	// Verify state consistency: counter processed same count as sink received.
	counterVal := counter.Count()
	sinkVal := sink.Count()

	// Counter is upstream of sink, so counter >= sink.
	if counterVal < sinkVal {
		t.Errorf("counter (%d) < sink (%d) — state inconsistency", counterVal, sinkVal)
	}

	// When pipeline stops cleanly, all messages should be fully drained.
	if sinkVal != numMessages {
		t.Errorf("expected %d messages at sink, got %d", numMessages, sinkVal)
	}
	if counterVal != numMessages {
		t.Errorf("expected counter=%d, got %d", numMessages, counterVal)
	}
}

// TestQuiescence_NoPrematureQuiescence verifies that quiescence is not declared
// prematurely. Uses a slow source with an artificial delay to ensure messages
// are still in-flight when the stop barrier is injected.
func TestQuiescence_NoPrematureQuiescence(t *testing.T) {
	numMessages := 50
	var quiescenceTime time.Time
	var lastMessageTime atomic.Value // stores time.Time

	// Track when the last message is processed at the sink.
	trackingSink := &trackingTimeSink{
		onProcess: func() {
			lastMessageTime.Store(time.Now())
		},
	}

	// Use a slow source to ensure messages are still being emitted
	// when we try to stop.
	source := &slowSourceHarness{
		items: makeItems(numMessages),
		delay: 2 * time.Millisecond,
	}

	pipeline, err := NewGraphBuilder("quiescence-no-premature").
		AddSource("source", source).
		AddOperator("op", &passthroughOp{}).
		AddSink("sink", trackingSink).
		Connect("source", "op").
		Connect("op", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Wait briefly, then stop — source should still be producing.
	time.Sleep(30 * time.Millisecond)

	if err := pipeline.Stop(ctx); err != nil {
		if pipeline.State() != PipelineStopped && pipeline.State() != PipelineStopping {
			t.Fatalf("Stop failed: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		quiescenceTime = time.Now()
	case <-time.After(10 * time.Second):
		stacks := captureGoroutineStacks()
		t.Fatalf("DEADLOCK: pipeline did not reach quiescence.\nStacks:\n%s", stacks)
	}

	// Quiescence should happen AFTER the last message was processed.
	if lmt := lastMessageTime.Load(); lmt != nil {
		lastMsg := lmt.(time.Time)
		if quiescenceTime.Before(lastMsg) {
			t.Error("quiescence detected BEFORE last message was processed — premature quiescence")
		}
	}

	// All messages that were in-flight should have been processed.
	count := trackingSink.Count()
	if count == 0 {
		t.Error("expected at least some messages to be processed")
	}

	// Pipeline should be in a terminal state.
	state := pipeline.State()
	if state != PipelineStopped {
		t.Errorf("expected Stopped state, got %s", state)
	}
}

// TestQuiescence_ControllerLevel_MultiSource verifies controller-level quiescence
// detection with multiple sources, ensuring quiescence requires ALL sources to
// stop and complete a checkpoint.
func TestQuiescence_ControllerLevel_MultiSource(t *testing.T) {
	nodeIDs := []string{"S1", "S2", "A", "Sink"}
	sourceIDs := []string{"S1", "S2"}
	sinkIDs := []string{"Sink"}

	var quiescent atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   nodeIDs,
		SourceIDs: sourceIDs,
		SinkIDs:   sinkIDs,
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Set up checkpoint.
	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch:          1,
		Status:         CheckpointInProgress,
		StartedAt:      time.Now(),
		CompletedNodes: make(map[string]bool),
		NodeStates:     make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	// Only source1 stops — should NOT trigger quiescence.
	ctrl.StoppedChan() <- NodeStopped{NodeID: "S1"}
	time.Sleep(50 * time.Millisecond)

	if quiescent.Load() {
		t.Error("quiescence should not be declared with only one source stopped")
	}

	// All nodes complete checkpoint.
	for _, id := range nodeIDs {
		ctrl.CompletionChan() <- NodeCompletion{NodeID: id, Epoch: 1}
	}
	time.Sleep(50 * time.Millisecond)

	// Checkpoint complete, but not all nodes stopped yet.
	if quiescent.Load() {
		t.Error("quiescence should not be declared with incomplete node stops")
	}

	// Remaining nodes stop.
	ctrl.StoppedChan() <- NodeStopped{NodeID: "S2"}
	ctrl.StoppedChan() <- NodeStopped{NodeID: "A"}
	ctrl.StoppedChan() <- NodeStopped{NodeID: "Sink"}
	time.Sleep(50 * time.Millisecond)

	if !quiescent.Load() {
		t.Error("expected quiescence after all nodes stopped and checkpoint completed")
	}
}

// TestQuiescence_ControllerLevel_NoPrematureWithDelay verifies that injecting an
// artificial delay between the barrier and the final message does not cause
// premature quiescence at the controller level.
func TestQuiescence_ControllerLevel_NoPrematureWithDelay(t *testing.T) {
	nodeIDs := []string{"S", "A", "B", "Sink"}

	var quiescent atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   nodeIDs,
		SourceIDs: []string{"S"},
		SinkIDs:   []string{"Sink"},
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Set up checkpoint.
	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch:          1,
		Status:         CheckpointInProgress,
		StartedAt:      time.Now(),
		CompletedNodes: make(map[string]bool),
		NodeStates:     make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	// Source stops and reports checkpoint.
	ctrl.StoppedChan() <- NodeStopped{NodeID: "S"}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "S", Epoch: 1}
	time.Sleep(50 * time.Millisecond)

	// Only source has stopped/completed. Other nodes still processing.
	if quiescent.Load() {
		t.Error("premature quiescence — nodes A, B, Sink still active")
	}

	// Simulate delay: node A completes checkpoint but B hasn't yet.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1}
	ctrl.StoppedChan() <- NodeStopped{NodeID: "A"}
	time.Sleep(100 * time.Millisecond) // artificial delay

	if quiescent.Load() {
		t.Error("premature quiescence — nodes B, Sink still active")
	}

	// B and Sink complete.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 1}
	ctrl.StoppedChan() <- NodeStopped{NodeID: "B"}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "Sink", Epoch: 1}
	ctrl.StoppedChan() <- NodeStopped{NodeID: "Sink"}
	time.Sleep(50 * time.Millisecond)

	if !quiescent.Load() {
		t.Error("expected quiescence after all nodes stopped and completed checkpoint")
	}
}

// trackingTimeSink records when each message is processed.
type trackingTimeSink struct {
	BaseOperator
	mu        sync.Mutex
	count     int
	onProcess func()
}

func (s *trackingTimeSink) ProcessMessage(_ context.Context, msg DataMessage, _ Collector) error {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
	if s.onProcess != nil {
		s.onProcess()
	}
	return nil
}

func (s *trackingTimeSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
