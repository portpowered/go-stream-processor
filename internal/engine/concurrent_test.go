package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentOperator_ProcessesInParallel(t *testing.T) {
	// Operator that records start/end times to prove parallel execution.
	var active int64
	var maxActive int64
	var mu sync.Mutex

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			cur := atomic.AddInt64(&active, 1)
			mu.Lock()
			if cur > maxActive {
				maxActive = cur
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)
			atomic.AddInt64(&active, -1)

			return collector.Emit(msg)
		},
	}

	concurrency := 4
	op := NewConcurrentOperator(inner, concurrency)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Send enough messages to saturate the pool.
	numMsgs := 8
	for i := 0; i < numMsgs; i++ {
		msg := DataMessage{Key: fmt.Sprintf("k%d", i), Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage(%d) error: %v", i, err)
		}
	}

	// Wait for all workers to finish.
	op.wg.Wait()

	mu.Lock()
	peak := maxActive
	mu.Unlock()

	if peak < 2 {
		t.Errorf("expected parallel execution (peak active >= 2), got %d", peak)
	}
	if peak > int64(concurrency) {
		t.Errorf("exceeded concurrency limit: peak %d > limit %d", peak, concurrency)
	}

	msgs := collector.Messages()
	if len(msgs) != numMsgs {
		t.Errorf("expected %d output messages, got %d", numMsgs, len(msgs))
	}
}

func TestConcurrentOperator_BackpressureWhenPoolFull(t *testing.T) {
	// Operator that blocks until released.
	blocker := make(chan struct{})
	var started int64

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			atomic.AddInt64(&started, 1)
			<-blocker
			return nil
		},
	}

	concurrency := 2
	op := NewConcurrentOperator(inner, concurrency)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Fill the pool.
	for i := 0; i < concurrency; i++ {
		msg := DataMessage{Key: "k", Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	// Wait for workers to actually start.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt64(&started) < int64(concurrency) {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for workers to start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Next send should block (pool full). Use a timeout to verify.
	done := make(chan struct{})
	go func() {
		msg := DataMessage{Key: "k", Value: "blocked"}
		_ = op.ProcessMessage(ctx, msg, collector)
		close(done)
	}()

	select {
	case <-done:
		t.Error("expected ProcessMessage to block when pool is full")
	case <-time.After(50 * time.Millisecond):
		// Good — it's blocking.
	}

	// Release all workers.
	close(blocker)

	// The blocked send should now complete.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("ProcessMessage didn't unblock after workers released")
	}

	op.wg.Wait()
}

func TestConcurrentOperator_BarrierWaitsForInFlight(t *testing.T) {
	var processed int64

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			time.Sleep(30 * time.Millisecond)
			atomic.AddInt64(&processed, 1)
			return collector.Emit(msg)
		},
		checkpointFn: func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
			return []byte("snapshot"), nil
		},
	}

	op := NewConcurrentOperator(inner, 4)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Send messages.
	for i := 0; i < 6; i++ {
		msg := DataMessage{Key: "k", Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	// HandleCheckpoint should wait for all in-flight workers.
	state, err := op.HandleCheckpoint(ctx, BarrierSignal{Epoch: 1})
	if err != nil {
		t.Fatalf("HandleCheckpoint error: %v", err)
	}

	// After checkpoint, all messages should be processed.
	count := atomic.LoadInt64(&processed)
	if count != 6 {
		t.Errorf("expected 6 processed messages after checkpoint, got %d", count)
	}

	if string(state) != "snapshot" {
		t.Errorf("expected state 'snapshot', got %q", string(state))
	}

	msgs := collector.Messages()
	if len(msgs) != 6 {
		t.Errorf("expected 6 output messages, got %d", len(msgs))
	}
}

func TestConcurrentOperator_ErrorPropagation(t *testing.T) {
	callCount := int64(0)

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			n := atomic.AddInt64(&callCount, 1)
			if n == 3 {
				return fmt.Errorf("worker error on message 3")
			}
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	op := NewConcurrentOperator(inner, 2)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Send messages; the error from worker 3 should surface.
	var gotErr error
	for i := 0; i < 6; i++ {
		msg := DataMessage{Key: "k", Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			gotErr = err
			break
		}
	}

	// The error might surface on ProcessMessage or on HandleCheckpoint/drainError.
	if gotErr == nil {
		// Wait for workers and check via HandleCheckpoint.
		_, err := op.HandleCheckpoint(ctx, BarrierSignal{Epoch: 1})
		gotErr = err
	}

	if gotErr == nil {
		t.Error("expected error from concurrent worker, got nil")
	}
}

func TestConcurrentOperator_ConcurrencyOf1IsSequential(t *testing.T) {
	var order []int
	var mu sync.Mutex

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			mu.Lock()
			order = append(order, msg.Value.(int))
			mu.Unlock()
			return nil
		},
	}

	op := NewConcurrentOperator(inner, 1)
	collector := newCollectingCollector()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		msg := DataMessage{Key: "k", Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	op.wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 5 {
		t.Fatalf("expected 5 processed, got %d", len(order))
	}
	// With concurrency=1, order should be preserved.
	for i, v := range order {
		if v != i {
			t.Errorf("order[%d] = %d, expected %d (sequential)", i, v, i)
		}
	}
}

func TestConcurrentOperator_ContextCancellation(t *testing.T) {
	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	op := NewConcurrentOperator(inner, 1)
	collector := newCollectingCollector()

	// Fill the single worker slot.
	ctx, cancel := context.WithCancel(context.Background())
	msg := DataMessage{Key: "k", Value: 1}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("first ProcessMessage error: %v", err)
	}

	// Cancel context while trying to acquire a second slot.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	msg2 := DataMessage{Key: "k", Value: 2}
	err := op.ProcessMessage(ctx, msg2, collector)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestConcurrentOperator_OnCloseWaitsForWorkers(t *testing.T) {
	var processed int64

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt64(&processed, 1)
			return nil
		},
	}

	op := NewConcurrentOperator(inner, 4)
	collector := newCollectingCollector()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		msg := DataMessage{Key: "k", Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	if err := op.OnClose(ctx); err != nil {
		t.Fatalf("OnClose error: %v", err)
	}

	if count := atomic.LoadInt64(&processed); count != 4 {
		t.Errorf("expected 4 processed after OnClose, got %d", count)
	}
}

func TestConcurrentOperator_DrainWaitsForInFlight(t *testing.T) {
	var processed int64
	concurrency := 4

	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			time.Sleep(30 * time.Millisecond)
			atomic.AddInt64(&processed, 1)
			return collector.Emit(msg)
		},
	}

	op := NewConcurrentOperator(inner, concurrency)
	collector := newCollectingCollector()
	ctx := context.Background()

	numMsgs := concurrency
	for i := 0; i < numMsgs; i++ {
		msg := DataMessage{Key: fmt.Sprintf("k%d", i), Value: i}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage(%d) error: %v", i, err)
		}
	}

	// Drain should block until all workers complete.
	if err := op.Drain(ctx); err != nil {
		t.Fatalf("Drain error: %v", err)
	}

	count := atomic.LoadInt64(&processed)
	if count != int64(numMsgs) {
		t.Errorf("expected %d processed after Drain, got %d", numMsgs, count)
	}

	msgs := collector.Messages()
	if len(msgs) != numMsgs {
		t.Errorf("expected %d output messages after Drain, got %d", numMsgs, len(msgs))
	}

	// Calling Drain again should be safe (idempotent).
	if err := op.Drain(ctx); err != nil {
		t.Fatalf("second Drain error: %v", err)
	}
}

func TestConcurrentOperator_DrainContextCancellation(t *testing.T) {
	inner := &testConcurrentInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			// Block for a long time — simulates slow work.
			time.Sleep(5 * time.Second)
			return nil
		},
	}

	op := NewConcurrentOperator(inner, 2)
	collector := newCollectingCollector()
	bgCtx := context.Background()

	// Start workers.
	for i := 0; i < 2; i++ {
		msg := DataMessage{Key: "k", Value: i}
		if err := op.ProcessMessage(bgCtx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	// Drain with a short-lived context.
	drainCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := op.Drain(drainCtx)
	if err == nil {
		t.Error("expected Drain to return error when context cancelled")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// --- Test helpers ---

type testConcurrentInner struct {
	BaseOperator
	processFn    func(ctx context.Context, msg DataMessage, collector Collector) error
	checkpointFn func(ctx context.Context, barrier BarrierSignal) ([]byte, error)
}

func (t *testConcurrentInner) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	if t.processFn != nil {
		return t.processFn(ctx, msg, collector)
	}
	return nil
}

func (t *testConcurrentInner) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	if t.checkpointFn != nil {
		return t.checkpointFn(ctx, barrier)
	}
	return nil, nil
}
