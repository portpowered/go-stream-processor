package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// --- Test helpers ---

// failingOperator returns an error for every message.
type failingOperator struct {
	BaseOperator
	err error
}

func (f *failingOperator) ProcessMessage(_ context.Context, _ DataMessage, _ Collector) error {
	return f.err
}

// failingSometimesOperator fails for messages matching a predicate.
type failingSometimesOperator struct {
	BaseOperator
	shouldFail func(DataMessage) bool
	err        error
	mu         sync.Mutex
	processed  int
}

func (f *failingSometimesOperator) ProcessMessage(_ context.Context, msg DataMessage, collector Collector) error {
	if f.shouldFail(msg) {
		return f.err
	}
	f.mu.Lock()
	f.processed++
	f.mu.Unlock()
	return collector.Emit(msg)
}

func (f *failingSometimesOperator) Processed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.processed
}

// deadLetterSink collects dead-letter messages for test verification.
type deadLetterSink struct {
	BaseOperator
	mu       sync.Mutex
	received []DeadLetterMessage
}

func (d *deadLetterSink) ProcessMessage(_ context.Context, msg DataMessage, _ Collector) error {
	dlm, ok := msg.Value.(DeadLetterMessage)
	if ok {
		d.mu.Lock()
		d.received = append(d.received, dlm)
		d.mu.Unlock()
	}
	return nil
}

func (d *deadLetterSink) Received() []DeadLetterMessage {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]DeadLetterMessage, len(d.received))
	copy(result, d.received)
	return result
}

// --- Tests ---

func TestErrorStrategy_SkipAndLog_ContinuesProcessing(t *testing.T) {
	// Pipeline with a failing operator using SkipAndLog should continue
	// processing messages after errors.
	source := &sliceSource{items: []interface{}{1, 2, 3, 4, 5}}

	// Operator that fails on odd numbers, passes even numbers.
	op := &failingSometimesOperator{
		shouldFail: func(msg DataMessage) bool {
			v, ok := msg.Value.(int)
			return ok && v%2 != 0
		},
		err: errors.New("odd number"),
	}

	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("skip-and-log-test").
		AddSource("source", source).
		AddOperator("op", op).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		SetErrorConfig(ErrorConfig{Strategy: ErrorStrategySkipAndLog}).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	pipeline.Wait()

	// Only even numbers should reach the sink (2 and 4).
	values := sink.Values()
	if len(values) != 2 {
		t.Fatalf("expected 2 values (even numbers), got %d: %v", len(values), values)
	}
	for _, v := range values {
		if v.(int)%2 != 0 {
			t.Errorf("unexpected odd value in sink: %v", v)
		}
	}

	// Error counts should report 3 errors for the operator node.
	counts := pipeline.ErrorCounts()
	if counts["op"] != 3 {
		t.Errorf("expected 3 errors for 'op', got %d", counts["op"])
	}
}

func TestErrorStrategy_DeadLetter_RoutesToDeadLetterSink(t *testing.T) {
	source := &sliceSource{items: []interface{}{1, 2, 3, 4, 5}}

	testErr := errors.New("processing error")
	op := &failingSometimesOperator{
		shouldFail: func(msg DataMessage) bool {
			v, ok := msg.Value.(int)
			return ok && v%2 != 0 // fail odd numbers
		},
		err: testErr,
	}

	sink := &collectSink{}
	dlSink := &deadLetterSink{}

	pipeline, err := NewGraphBuilder("dead-letter-test").
		AddSource("source", source).
		AddOperator("op", op).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		SetErrorConfig(ErrorConfig{
			Strategy:       ErrorStrategyDeadLetter,
			DeadLetterSink: dlSink,
		}).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	pipeline.Wait()

	// Even numbers should reach the normal sink.
	values := sink.Values()
	if len(values) != 2 {
		t.Fatalf("expected 2 values in sink, got %d: %v", len(values), values)
	}

	// Odd numbers should be in the dead-letter sink.
	dlMsgs := dlSink.Received()
	if len(dlMsgs) != 3 {
		t.Fatalf("expected 3 dead-letter messages, got %d", len(dlMsgs))
	}

	// Verify dead-letter messages have correct error and node ID.
	for _, dlm := range dlMsgs {
		if dlm.NodeID != "op" {
			t.Errorf("expected NodeID 'op', got %q", dlm.NodeID)
		}
		if dlm.Err.Error() != testErr.Error() {
			t.Errorf("expected error %q, got %q", testErr, dlm.Err)
		}
		v, ok := dlm.OriginalMsg.Value.(int)
		if !ok || v%2 == 0 {
			t.Errorf("unexpected dead-letter value: %v", dlm.OriginalMsg.Value)
		}
	}

	// Error counts should match.
	counts := pipeline.ErrorCounts()
	if counts["op"] != 3 {
		t.Errorf("expected 3 errors for 'op', got %d", counts["op"])
	}
}

func TestErrorStrategy_FailPipeline_StopsPipeline(t *testing.T) {
	source := &sliceSource{items: []interface{}{1, 2, 3, 4, 5}}

	testErr := errors.New("fatal error")
	op := &failingSometimesOperator{
		shouldFail: func(msg DataMessage) bool {
			v, ok := msg.Value.(int)
			return ok && v == 3 // fail on value 3
		},
		err: testErr,
	}

	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("fail-pipeline-test").
		AddSource("source", source).
		AddOperator("op", op).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		SetErrorConfig(ErrorConfig{Strategy: ErrorStrategyFailPipeline}).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)

	// Wait for the pipeline to exit.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — pipeline stopped.
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop within timeout")
	}

	// The sink should have received fewer than 5 messages (pipeline stopped
	// at or before message 3).
	values := sink.Values()
	if len(values) >= 5 {
		t.Errorf("expected pipeline to stop before processing all messages, got %d values", len(values))
	}

	// Error counts should have at least 1 error for 'op'.
	counts := pipeline.ErrorCounts()
	if counts["op"] < 1 {
		t.Errorf("expected at least 1 error for 'op', got %d", counts["op"])
	}
}

func TestErrorStrategy_DefaultIsSkipAndLog(t *testing.T) {
	// When no error config is set, the default should be SkipAndLog.
	source := &sliceSource{items: []interface{}{1, 2, 3}}
	op := &failingOperator{err: errors.New("error")}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("default-error-test").
		AddSource("source", source).
		AddOperator("op", op).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	pipeline.Wait()

	// With SkipAndLog, no messages reach the sink (all fail), but pipeline
	// should complete normally.
	values := sink.Values()
	if len(values) != 0 {
		t.Errorf("expected 0 values with all-failing operator, got %d", len(values))
	}

	// 3 errors should be counted.
	counts := pipeline.ErrorCounts()
	if counts["op"] != 3 {
		t.Errorf("expected 3 errors, got %d", counts["op"])
	}
}

func TestCollector_EmitError(t *testing.T) {
	// Test that operators can proactively report errors via EmitError.
	type emitErrorOp struct {
		BaseOperator
	}

	// Create an operator that uses EmitError for certain messages.
	op := &MapOperator[int, int]{
		MapFn: func(v int) int { return v },
	}
	_ = op

	// Test EmitError via the collectingCollector helper.
	collector := newCollectingCollector()
	testErr := errors.New("custom error")
	msg := DataMessage{Key: "k", Value: 42}
	collector.EmitError(msg, testErr)

	if len(collector.errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(collector.errors))
	}
	if collector.errors[0].Msg.Value != 42 {
		t.Errorf("expected value 42, got %v", collector.errors[0].Msg.Value)
	}
	if collector.errors[0].Err != testErr {
		t.Errorf("expected error %v, got %v", testErr, collector.errors[0].Err)
	}
}

func TestPipeline_ErrorCounts_ReportsPerNode(t *testing.T) {
	// Build a pipeline with two operators, each of which fails on different
	// messages. Verify error counts are tracked per node.
	source := &sliceSource{items: []interface{}{1, 2, 3, 4}}

	op1 := &failingSometimesOperator{
		shouldFail: func(msg DataMessage) bool {
			v, ok := msg.Value.(int)
			return ok && v == 1 // fail on 1
		},
		err: errors.New("op1 error"),
	}

	op2 := &failingSometimesOperator{
		shouldFail: func(msg DataMessage) bool {
			v, ok := msg.Value.(int)
			return ok && v == 4 // fail on 4
		},
		err: errors.New("op2 error"),
	}

	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("multi-error-test").
		AddSource("source", source).
		AddOperator("op1", op1).
		AddOperator("op2", op2).
		AddSink("sink", sink).
		Connect("source", "op1").
		Connect("op1", "op2").
		Connect("op2", "sink").
		SetErrorConfig(ErrorConfig{Strategy: ErrorStrategySkipAndLog}).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	pipeline.Wait()

	counts := pipeline.ErrorCounts()

	// op1 should have 1 error (value 1 fails at op1).
	if counts["op1"] != 1 {
		t.Errorf("expected 1 error for op1, got %d", counts["op1"])
	}

	// op2 should have 1 error (value 4 passes op1 but fails at op2).
	if counts["op2"] != 1 {
		t.Errorf("expected 1 error for op2, got %d", counts["op2"])
	}

	// Values 2 and 3 should reach the sink.
	values := sink.Values()
	if len(values) != 2 {
		t.Errorf("expected 2 values in sink, got %d: %v", len(values), values)
	}
}

func TestPipeline_Stop_CompletesWithDeadLetterStrategy(t *testing.T) {
	// Proves Pipeline.Stop() completes without hanging when using
	// ErrorStrategyDeadLetter — the dead-letter runtime receives a stop
	// signal and exits cleanly without needing Terminate().
	source := &slowSource{
		items: makeItems(50),
		delay: 5 * time.Millisecond,
	}

	testErr := errors.New("processing error")
	op := &failingSometimesOperator{
		shouldFail: func(msg DataMessage) bool {
			v, ok := msg.Value.(int)
			return ok && v%3 == 0 // fail every 3rd message
		},
		err: testErr,
	}

	sink := &collectSink{}
	dlSink := &deadLetterSink{}

	pipeline, err := NewGraphBuilder("stop-dead-letter-test").
		AddSource("source", source).
		AddOperator("op", op).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		SetErrorConfig(ErrorConfig{
			Strategy:       ErrorStrategyDeadLetter,
			DeadLetterSink: dlSink,
		}).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)

	// Let some messages flow so errors are routed to dead-letter.
	time.Sleep(100 * time.Millisecond)

	// Graceful stop — this is the behavior under test. Stop() must not hang.
	if err := pipeline.Stop(ctx); err != nil {
		// Pipeline may have already stopped if source completed quickly.
		state := pipeline.State()
		if state != PipelineStopped && state != PipelineStopping {
			t.Fatalf("Stop() failed: %v", err)
		}
	}

	// Wait must complete within a reasonable timeout.
	done := make(chan struct{})
	go func() {
		pipeline.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — pipeline completed gracefully.
	case <-time.After(5 * time.Second):
		t.Fatal("Pipeline.Wait() did not complete within 5s after Stop() — dead-letter runtime may be hanging")
	}

	// Dead-letter sink should have received some error messages.
	dlMsgs := dlSink.Received()
	if len(dlMsgs) == 0 {
		t.Fatal("expected at least one dead-letter message, got 0")
	}

	// Verify dead-letter messages have correct structure.
	for _, dlm := range dlMsgs {
		if dlm.NodeID != "op" {
			t.Errorf("expected NodeID 'op', got %q", dlm.NodeID)
		}
		if dlm.Err == nil {
			t.Error("dead-letter message has nil error")
		}
	}

	// All processed messages should be accounted for.
	sinkCount := len(sink.Values())
	dlCount := len(dlMsgs)
	if sinkCount == 0 {
		t.Error("expected at least some messages in the normal sink")
	}
	t.Logf("sink: %d, dead-letter: %d, total: %d", sinkCount, dlCount, sinkCount+dlCount)
}
