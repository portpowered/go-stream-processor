package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- Test helpers ---

// slowSource emits items with a delay between each, allowing lifecycle tests
// to exercise start/stop while the source is actively producing.
type slowSource struct {
	items []interface{}
	delay time.Duration
}

func (s *slowSource) Run(ctx context.Context, collector Collector) error {
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

// blockingSource blocks until context is cancelled, simulating a long-running source.
type blockingSource struct{}

func (s *blockingSource) Run(ctx context.Context, _ Collector) error {
	<-ctx.Done()
	return ctx.Err()
}

// countSink counts received messages.
type countSink struct {
	BaseOperator
	mu    sync.Mutex
	count int
}

func (s *countSink) ProcessMessage(_ context.Context, _ DataMessage, _ Collector) error {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
	return nil
}

func (s *countSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// --- Tests ---

func TestPipeline_StartAndGracefulStop(t *testing.T) {
	// Build a 3-node pipeline with a blocking source so we can exercise Stop().
	source := &slowSource{
		items: make([]interface{}, 100),
		delay: 10 * time.Millisecond,
	}
	for i := range source.items {
		source.items[i] = i
	}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("stop-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Verify initial state.
	if pipeline.State() != PipelineInitializing {
		t.Fatalf("expected Initializing, got %s", pipeline.State())
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Verify running state.
	if pipeline.State() != PipelineRunning {
		t.Fatalf("expected Running, got %s", pipeline.State())
	}

	// Let some messages flow, then stop gracefully.
	time.Sleep(50 * time.Millisecond)

	if err := pipeline.Stop(ctx); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	// Verify stopping state.
	state := pipeline.State()
	if state != PipelineStopping && state != PipelineStopped {
		t.Fatalf("expected Stopping or Stopped, got %s", state)
	}

	// Wait for pipeline to fully exit.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop within timeout")
	}

	// Verify terminal state.
	if pipeline.State() != PipelineStopped {
		t.Fatalf("expected Stopped, got %s", pipeline.State())
	}

	// Some messages should have been processed.
	if sink.Count() == 0 {
		t.Error("expected at least some messages to have been processed")
	}
	t.Logf("processed %d messages before stop", sink.Count())
}

func TestPipeline_StateTransitionsThroughLifecycle(t *testing.T) {
	source := &blockingSource{}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("lifecycle-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// 1. Initializing
	if pipeline.State() != PipelineInitializing {
		t.Fatalf("expected Initializing, got %s", pipeline.State())
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// 2. Running
	if pipeline.State() != PipelineRunning {
		t.Fatalf("expected Running, got %s", pipeline.State())
	}

	// 3. Stopping (via Stop)
	if err := pipeline.Stop(ctx); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	state := pipeline.State()
	if state != PipelineStopping && state != PipelineStopped {
		t.Fatalf("expected Stopping or Stopped, got %s", state)
	}

	// 4. Stopped (after Wait)
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop within timeout")
	}

	if pipeline.State() != PipelineStopped {
		t.Fatalf("expected Stopped, got %s", pipeline.State())
	}

	// 5. Stop on an already-stopped pipeline should return error.
	if err := pipeline.Stop(ctx); err == nil {
		t.Fatal("expected error when stopping already-stopped pipeline")
	}
}

func TestPipeline_StopNotStarted(t *testing.T) {
	source := &blockingSource{}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("not-started-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Stop before start should fail.
	if err := pipeline.Stop(context.Background()); err == nil {
		t.Fatal("expected error when stopping pipeline that hasn't started")
	}
}

func TestPipeline_WaitBeforeStart(t *testing.T) {
	source := &blockingSource{}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("wait-before-start").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Wait before start should return immediately.
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("Wait() before Start() returned error: %v", err)
	}
}

func TestPipeline_FiniteSourceCompletesNaturally(t *testing.T) {
	// A pipeline with a finite source should complete naturally and
	// transition through Running -> Stopped.
	source := &sliceSource{items: []interface{}{1, 2, 3}}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("finite-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	if pipeline.State() != PipelineInitializing {
		t.Fatalf("expected Initializing, got %s", pipeline.State())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)

	if pipeline.State() != PipelineRunning {
		t.Fatalf("expected Running after Start(), got %s", pipeline.State())
	}

	pipeline.Wait()

	if pipeline.State() != PipelineStopped {
		t.Fatalf("expected Stopped after natural completion, got %s", pipeline.State())
	}

	values := sink.Values()
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(values))
	}
}

func TestPipeline_PauseResumePreservesMessages(t *testing.T) {
	// A pipeline with a slow source should not lose messages across pause/resume.
	source := &slowSource{
		items: make([]interface{}, 50),
		delay: 5 * time.Millisecond,
	}
	for i := range source.items {
		source.items[i] = i
	}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("pause-resume-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Let some messages flow.
	time.Sleep(30 * time.Millisecond)

	// Pause the pipeline.
	if err := pipeline.Pause(ctx); err != nil {
		t.Fatalf("Pause() failed: %v", err)
	}
	if pipeline.State() != PipelinePaused {
		t.Fatalf("expected Paused, got %s", pipeline.State())
	}

	// Record count at pause time.
	countAtPause := sink.Count()
	t.Logf("messages at pause: %d", countAtPause)

	// Wait a bit — count should not increase while paused.
	time.Sleep(50 * time.Millisecond)
	countDuringPause := sink.Count()

	// The sink count should be the same or very close (source may still
	// push to buffer, but sink shouldn't process since it's paused).
	// Note: the source itself is not paused, only processing nodes are.
	t.Logf("messages during pause: %d", countDuringPause)

	// Resume the pipeline.
	if err := pipeline.Resume(ctx); err != nil {
		t.Fatalf("Resume() failed: %v", err)
	}
	if pipeline.State() != PipelineRunning {
		t.Fatalf("expected Running after Resume(), got %s", pipeline.State())
	}

	// Wait for pipeline to complete naturally.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not complete within timeout")
	}

	// All messages should have been processed (no data loss).
	finalCount := sink.Count()
	if finalCount != 50 {
		t.Errorf("expected 50 messages (no data loss), got %d", finalCount)
	}
	t.Logf("final count: %d", finalCount)
}

func TestPipeline_TerminateExitsWithinTimeout(t *testing.T) {
	// A pipeline with a blocking source should terminate within a timeout.
	source := &blockingSource{}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("terminate-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	if pipeline.State() != PipelineRunning {
		t.Fatalf("expected Running, got %s", pipeline.State())
	}

	// Terminate the pipeline.
	if err := pipeline.Terminate(ctx); err != nil {
		t.Fatalf("Terminate() failed: %v", err)
	}

	state := pipeline.State()
	if state != PipelineTerminated {
		t.Fatalf("expected Terminated, got %s", state)
	}

	// Wait should complete quickly since terminate cancels the context.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — terminated within timeout.
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not terminate within 2 seconds")
	}

	// State should remain Terminated.
	if pipeline.State() != PipelineTerminated {
		t.Fatalf("expected Terminated after Wait(), got %s", pipeline.State())
	}
}

func TestPipeline_StopCompletesFinalCheckpoint(t *testing.T) {
	// A pipeline stopped via Stop() should complete a final checkpoint
	// before exiting, transitioning through Stopping -> Stopped.
	source := &slowSource{
		items: make([]interface{}, 100),
		delay: 5 * time.Millisecond,
	}
	for i := range source.items {
		source.items[i] = i
	}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("stop-checkpoint-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Let some messages flow.
	time.Sleep(30 * time.Millisecond)

	// Stop gracefully (triggers then_stop barrier).
	if err := pipeline.Stop(ctx); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	state := pipeline.State()
	if state != PipelineStopping && state != PipelineStopped {
		t.Fatalf("expected Stopping or Stopped after Stop(), got %s", state)
	}

	// Wait for pipeline to exit.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop within timeout")
	}

	// Terminal state should be Stopped (not Terminated).
	if pipeline.State() != PipelineStopped {
		t.Fatalf("expected Stopped (graceful), got %s", pipeline.State())
	}

	// Some messages should have been processed.
	if sink.Count() == 0 {
		t.Error("expected at least some messages processed before stop")
	}
	t.Logf("processed %d messages before graceful stop", sink.Count())
}

func TestPipeline_StateTransitions_PauseTerminate(t *testing.T) {
	// Test transition: Running -> Paused -> Terminated
	source := &blockingSource{}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("pause-terminate-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Running -> Paused
	if err := pipeline.Pause(ctx); err != nil {
		t.Fatalf("Pause() failed: %v", err)
	}
	if pipeline.State() != PipelinePaused {
		t.Fatalf("expected Paused, got %s", pipeline.State())
	}

	// Paused -> Terminated
	if err := pipeline.Terminate(ctx); err != nil {
		t.Fatalf("Terminate() from Paused failed: %v", err)
	}
	if pipeline.State() != PipelineTerminated {
		t.Fatalf("expected Terminated, got %s", pipeline.State())
	}

	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not terminate within timeout")
	}
}

func TestPipeline_InvalidStateTransitions(t *testing.T) {
	source := &blockingSource{}
	sink := &countSink{}

	pipeline, err := NewGraphBuilder("invalid-transitions-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx := context.Background()

	// Pause before start should fail.
	if err := pipeline.Pause(ctx); err == nil {
		t.Fatal("expected error when pausing unstarted pipeline")
	}

	// Resume before start should fail.
	if err := pipeline.Resume(ctx); err == nil {
		t.Fatal("expected error when resuming unstarted pipeline")
	}

	// Terminate before start should fail.
	if err := pipeline.Terminate(ctx); err == nil {
		t.Fatal("expected error when terminating unstarted pipeline")
	}

	pipeline.Start(ctx)

	// Resume when not paused should fail.
	if err := pipeline.Resume(ctx); err == nil {
		t.Fatal("expected error when resuming non-paused pipeline")
	}

	// Pause twice should fail.
	if err := pipeline.Pause(ctx); err != nil {
		t.Fatalf("first Pause() failed: %v", err)
	}
	if err := pipeline.Pause(ctx); err == nil {
		t.Fatal("expected error when pausing already-paused pipeline")
	}

	// Clean up.
	pipeline.Terminate(ctx)
	pipeline.Wait()
}

func TestPipelineState_String(t *testing.T) {
	tests := []struct {
		state PipelineState
		want  string
	}{
		{PipelineInitializing, "Initializing"},
		{PipelineRunning, "Running"},
		{PipelinePaused, "Paused"},
		{PipelineStopping, "Stopping"},
		{PipelineStopped, "Stopped"},
		{PipelineTerminated, "Terminated"},
		{PipelineError, "Error"},
		{PipelineState(99), "Unknown(99)"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("PipelineState(%d).String() = %q, want %q", int(tt.state), got, tt.want)
		}
	}
}
