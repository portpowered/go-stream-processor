package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- timerManager unit tests ---

func TestTimerManager_RegisterAndPeek(t *testing.T) {
	tm := newTimerManager()

	now := time.Now()
	tm.Register("a", now.Add(100*time.Millisecond))
	tm.Register("b", now.Add(50*time.Millisecond))
	tm.Register("c", now.Add(200*time.Millisecond))

	if tm.Len() != 3 {
		t.Fatalf("expected 3 timers, got %d", tm.Len())
	}

	// Peek should return the earliest timer.
	next, ok := tm.Peek()
	if !ok {
		t.Fatal("expected Peek to return true")
	}
	if next.Name != "b" {
		t.Errorf("expected earliest timer 'b', got %q", next.Name)
	}
}

func TestTimerManager_PopExpired(t *testing.T) {
	tm := newTimerManager()

	now := time.Now()
	tm.Register("past1", now.Add(-10*time.Millisecond))
	tm.Register("past2", now.Add(-5*time.Millisecond))
	tm.Register("future", now.Add(1*time.Hour))

	expired := tm.PopExpired(now)
	if len(expired) != 2 {
		t.Fatalf("expected 2 expired, got %d", len(expired))
	}
	if expired[0].Name != "past1" {
		t.Errorf("expected 'past1', got %q", expired[0].Name)
	}
	if expired[1].Name != "past2" {
		t.Errorf("expected 'past2', got %q", expired[1].Name)
	}

	if tm.Len() != 1 {
		t.Errorf("expected 1 remaining, got %d", tm.Len())
	}
}

func TestTimerManager_PopExpired_NoneExpired(t *testing.T) {
	tm := newTimerManager()
	tm.Register("future", time.Now().Add(1*time.Hour))

	expired := tm.PopExpired(time.Now())
	if len(expired) != 0 {
		t.Errorf("expected 0 expired, got %d", len(expired))
	}
}

func TestTimerManager_SnapshotRestore(t *testing.T) {
	tm := newTimerManager()

	now := time.Now().Truncate(time.Nanosecond) // truncate for round-trip
	tm.Register("timer-a", now.Add(100*time.Millisecond))
	tm.Register("timer-b", now.Add(200*time.Millisecond))
	tm.Register("timer-c", now.Add(50*time.Millisecond))

	data := tm.Snapshot()
	if data == nil {
		t.Fatal("expected non-nil snapshot")
	}

	// Restore into a new manager.
	tm2 := newTimerManager()
	if err := tm2.Restore(data); err != nil {
		t.Fatalf("Restore error: %v", err)
	}

	if tm2.Len() != 3 {
		t.Fatalf("expected 3 restored timers, got %d", tm2.Len())
	}

	// Timers should be in sorted order: c, a, b.
	next, _ := tm2.Peek()
	if next.Name != "timer-c" {
		t.Errorf("expected earliest restored timer 'timer-c', got %q", next.Name)
	}
}

func TestTimerManager_SnapshotEmpty(t *testing.T) {
	tm := newTimerManager()
	data := tm.Snapshot()
	if data != nil {
		t.Errorf("expected nil snapshot for empty manager, got %v", data)
	}

	// Restore from nil should work.
	if err := tm.Restore(nil); err != nil {
		t.Fatalf("Restore(nil) error: %v", err)
	}
	if tm.Len() != 0 {
		t.Errorf("expected 0 timers after restoring nil, got %d", tm.Len())
	}
}

// --- Timer checkpoint encoding tests ---

func TestTimerCheckpointEncodeDecode(t *testing.T) {
	opState := []byte("operator-state-data")
	timerState := []byte("timer-state-data")

	combined := encodeTimerCheckpoint(opState, timerState)
	gotOp, gotTimer, err := decodeTimerCheckpoint(combined)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if string(gotOp) != "operator-state-data" {
		t.Errorf("expected operator state 'operator-state-data', got %q", string(gotOp))
	}
	if string(gotTimer) != "timer-state-data" {
		t.Errorf("expected timer state 'timer-state-data', got %q", string(gotTimer))
	}
}

func TestTimerCheckpointEncodeDecode_NoTimerState(t *testing.T) {
	opState := []byte("op-only")
	combined := encodeTimerCheckpoint(opState, nil)

	gotOp, gotTimer, err := decodeTimerCheckpoint(combined)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if string(gotOp) != "op-only" {
		t.Errorf("expected 'op-only', got %q", string(gotOp))
	}
	if gotTimer != nil {
		t.Errorf("expected nil timer state, got %v", gotTimer)
	}
}

func TestTimerCheckpointEncodeDecode_NoOpState(t *testing.T) {
	timerState := []byte("timer-only")
	combined := encodeTimerCheckpoint(nil, timerState)

	gotOp, gotTimer, err := decodeTimerCheckpoint(combined)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if gotOp != nil {
		t.Errorf("expected nil op state, got %v", gotOp)
	}
	if string(gotTimer) != "timer-only" {
		t.Errorf("expected 'timer-only', got %q", string(gotTimer))
	}
}

// --- Timer fires at correct time (integration test with runtime) ---

// testTimerOp is a test helper that implements TimerOperator.
type testTimerOp struct {
	BaseOperator
	mu       sync.Mutex
	timers   []string
	timerAt  map[string]time.Time
	onMsg    func(ctx context.Context, msg DataMessage, collector Collector) error
	onTimer  func(ctx context.Context, name string, fireAt time.Time, collector Collector) error
	stateVal []byte
}

func newTestTimerOp() *testTimerOp {
	return &testTimerOp{
		timerAt: make(map[string]time.Time),
	}
}

func (t *testTimerOp) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	if t.onMsg != nil {
		return t.onMsg(ctx, msg, collector)
	}
	return nil
}

func (t *testTimerOp) HandleTimer(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
	t.mu.Lock()
	t.timers = append(t.timers, name)
	t.timerAt[name] = fireAt
	t.mu.Unlock()

	if t.onTimer != nil {
		return t.onTimer(ctx, name, fireAt, collector)
	}
	return nil
}

func (t *testTimerOp) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	return t.stateVal, nil
}

func (t *testTimerOp) FiredTimers() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]string, len(t.timers))
	copy(result, t.timers)
	return result
}

func TestTimerFiresAtCorrectTime(t *testing.T) {
	op := newTestTimerOp()
	op.onMsg = func(ctx context.Context, msg DataMessage, collector Collector) error {
		// Register a timer 50ms from now.
		collector.RegisterTimer("test-timer", time.Now().Add(50*time.Millisecond))
		return nil
	}
	op.onTimer = func(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
		return collector.Emit(DataMessage{Key: "timer", Value: name})
	}

	node := NewOperatorNode("timer-node", op)
	inBuf := NewBuffer(16)
	outBuf := NewBuffer(16)
	node.SetOutputs([]*Buffer{outBuf})

	completionCh := make(chan NodeCompletion, 16)
	stoppedCh := make(chan NodeStopped, 16)

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:         node,
		NodeType:     NodeTypeOperator,
		Inputs:       []*Buffer{inBuf},
		Outputs:      []*Buffer{outBuf},
		CompletionCh: completionCh,
		StoppedCh:    stoppedCh,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send a data message to trigger timer registration.
	start := time.Now()
	inBuf.Send(ctx, NewDataMessage("k", "trigger", time.Now()))

	// Wait for the timer to fire and produce output.
	msg, err := outBuf.Recv(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if !msg.IsData() {
		t.Fatal("expected data message")
	}
	if msg.Data.Value != "test-timer" {
		t.Errorf("expected timer name 'test-timer', got %v", msg.Data.Value)
	}

	// Timer should have fired roughly 50ms after registration.
	if elapsed < 40*time.Millisecond {
		t.Errorf("timer fired too early: %v", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("timer fired too late: %v", elapsed)
	}

	rt.Stop()
	rt.Wait()
}

func TestTimerMultipleConcurrentTimers(t *testing.T) {
	op := newTestTimerOp()
	op.onMsg = func(ctx context.Context, msg DataMessage, collector Collector) error {
		// Register 3 timers with different delays.
		collector.RegisterTimer("fast", time.Now().Add(20*time.Millisecond))
		collector.RegisterTimer("medium", time.Now().Add(60*time.Millisecond))
		collector.RegisterTimer("slow", time.Now().Add(100*time.Millisecond))
		return nil
	}
	op.onTimer = func(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
		return collector.Emit(DataMessage{Key: "timer", Value: name})
	}

	node := NewOperatorNode("multi-timer", op)
	inBuf := NewBuffer(16)
	outBuf := NewBuffer(16)
	node.SetOutputs([]*Buffer{outBuf})

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{inBuf},
		Outputs:  []*Buffer{outBuf},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send trigger message.
	inBuf.Send(ctx, NewDataMessage("k", "go", time.Now()))

	// Collect 3 timer outputs.
	var names []string
	for i := 0; i < 3; i++ {
		msg, err := outBuf.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv %d error: %v", i, err)
		}
		names = append(names, msg.Data.Value.(string))
	}

	// Timers should fire in order: fast, medium, slow.
	expected := []string{"fast", "medium", "slow"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("timer %d: expected %q, got %q", i, expected[i], name)
		}
	}

	rt.Stop()
	rt.Wait()
}

func TestTimerSurvivesCheckpointAndRecovery(t *testing.T) {
	// Phase 1: Create a timer, checkpoint before it fires.
	op1 := newTestTimerOp()
	op1.stateVal = []byte("my-operator-state")

	node1 := NewOperatorNode("timer-node", op1)
	outBuf1 := NewBuffer(16)
	node1.SetOutputs([]*Buffer{outBuf1})

	// Register a timer via the collector manually.
	collector1 := node1.newCollector(context.Background())
	futureTime := time.Now().Add(1 * time.Hour) // far in the future so it doesn't fire
	collector1.RegisterTimer("survive-timer", futureTime)

	// Verify timer is registered.
	if node1.timerMgr.Len() != 1 {
		t.Fatalf("expected 1 timer, got %d", node1.timerMgr.Len())
	}

	// Checkpoint.
	state, err := node1.HandleCheckpoint(context.Background(), BarrierSignal{Epoch: 1})
	if err != nil {
		t.Fatalf("HandleCheckpoint error: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil checkpoint state")
	}

	// Phase 2: Create a new node and restore from checkpoint.
	op2 := newTestTimerOp()
	op2.onTimer = func(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
		return collector.Emit(DataMessage{Key: "recovered", Value: name})
	}

	node2 := NewOperatorNode("timer-node", op2)
	outBuf2 := NewBuffer(16)
	node2.SetOutputs([]*Buffer{outBuf2})

	// Restore timer state.
	opState, err := node2.RestoreTimerState(state)
	if err != nil {
		t.Fatalf("RestoreTimerState error: %v", err)
	}

	// Operator state should be preserved.
	if string(opState) != "my-operator-state" {
		t.Errorf("expected operator state 'my-operator-state', got %q", string(opState))
	}

	// Timer should be restored.
	if node2.timerMgr.Len() != 1 {
		t.Fatalf("expected 1 restored timer, got %d", node2.timerMgr.Len())
	}

	next, ok := node2.timerMgr.Peek()
	if !ok {
		t.Fatal("expected timer after restore")
	}
	if next.Name != "survive-timer" {
		t.Errorf("expected timer name 'survive-timer', got %q", next.Name)
	}
	if !next.FireAt.Equal(futureTime.Truncate(time.Nanosecond)) {
		// UnixNano round-trip may lose sub-nanosecond precision on some platforms.
		diff := next.FireAt.Sub(futureTime)
		if diff > time.Microsecond || diff < -time.Microsecond {
			t.Errorf("timer fireAt differs by %v", diff)
		}
	}

	// Phase 3: Verify the timer fires in the new runtime.
	// Modify the restored timer to fire immediately for testing.
	node2.timerMgr.mu.Lock()
	if len(node2.timerMgr.timers) > 0 {
		node2.timerMgr.timers[0].FireAt = time.Now().Add(-1 * time.Millisecond)
	}
	node2.timerMgr.mu.Unlock()

	inBuf2 := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node2,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{inBuf2},
		Outputs:  []*Buffer{outBuf2},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// The restored timer should fire immediately.
	msg, err := outBuf2.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if !msg.IsData() {
		t.Fatal("expected data message from recovered timer")
	}
	if msg.Data.Value != "survive-timer" {
		t.Errorf("expected recovered timer name 'survive-timer', got %v", msg.Data.Value)
	}

	rt.Stop()
	rt.Wait()
}

// --- SleepOperator tests ---

func TestSleepOperator_DelaysMessages(t *testing.T) {
	delay := 80 * time.Millisecond
	op := NewSleepOperator(delay)

	node := NewOperatorNode("sleeper", op)
	inBuf := NewBuffer(16)
	outBuf := NewBuffer(16)
	node.SetOutputs([]*Buffer{outBuf})

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{inBuf},
		Outputs:  []*Buffer{outBuf},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send a message.
	start := time.Now()
	inBuf.Send(ctx, NewDataMessage("k", "delayed-msg", time.Now()))

	// Wait for the delayed output.
	msg, err := outBuf.Recv(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if !msg.IsData() {
		t.Fatal("expected data message")
	}
	if msg.Data.Value != "delayed-msg" {
		t.Errorf("expected 'delayed-msg', got %v", msg.Data.Value)
	}

	// Should have been delayed by approximately the configured duration.
	if elapsed < 60*time.Millisecond {
		t.Errorf("message arrived too early: %v (expected ~%v delay)", elapsed, delay)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("message arrived too late: %v", elapsed)
	}

	rt.Stop()
	rt.Wait()
}

func TestSleepOperator_MultipleConcurrentDelays(t *testing.T) {
	delay := 50 * time.Millisecond
	op := NewSleepOperator(delay)

	node := NewOperatorNode("sleeper", op)
	inBuf := NewBuffer(16)
	outBuf := NewBuffer(16)
	node.SetOutputs([]*Buffer{outBuf})

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{inBuf},
		Outputs:  []*Buffer{outBuf},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send 3 messages rapidly.
	for i := 0; i < 3; i++ {
		inBuf.Send(ctx, NewDataMessage("k", i, time.Now()))
	}

	// All 3 should arrive after the delay.
	var values []int
	for i := 0; i < 3; i++ {
		msg, err := outBuf.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv %d error: %v", i, err)
		}
		values = append(values, msg.Data.Value.(int))
	}

	if len(values) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(values))
	}

	// Values should be 0, 1, 2 (in order since timers fire in registration order).
	for i, v := range values {
		if v != i {
			t.Errorf("message %d: expected %d, got %d", i, i, v)
		}
	}

	rt.Stop()
	rt.Wait()
}

// --- OperatorNode HandleCheckpoint with timer wrapping ---

func TestOperatorNode_HandleCheckpointWithTimers(t *testing.T) {
	op := newTestTimerOp()
	op.stateVal = []byte("op-state")

	node := NewOperatorNode("n", op)
	node.SetOutputs([]*Buffer{})

	// Register some timers.
	col := node.newCollector(context.Background())
	col.RegisterTimer("t1", time.Now().Add(1*time.Minute))
	col.RegisterTimer("t2", time.Now().Add(2*time.Minute))

	// Checkpoint should include both operator and timer state.
	state, err := node.HandleCheckpoint(context.Background(), BarrierSignal{Epoch: 1})
	if err != nil {
		t.Fatalf("HandleCheckpoint error: %v", err)
	}

	// Decode and verify.
	opState, timerState, err := decodeTimerCheckpoint(state)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if string(opState) != "op-state" {
		t.Errorf("expected op state 'op-state', got %q", string(opState))
	}
	if timerState == nil {
		t.Fatal("expected non-nil timer state")
	}

	// Restore timers and verify.
	tm := newTimerManager()
	if err := tm.Restore(timerState); err != nil {
		t.Fatalf("timer Restore error: %v", err)
	}
	if tm.Len() != 2 {
		t.Errorf("expected 2 restored timers, got %d", tm.Len())
	}
}

func TestOperatorNode_HandleCheckpointWithoutTimers(t *testing.T) {
	// Non-timer operator should return raw operator state (no wrapping).
	op := &MapOperator[int, int]{MapFn: func(v int) int { return v }}
	node := NewOperatorNode("n", op)

	state, err := node.HandleCheckpoint(context.Background(), BarrierSignal{Epoch: 1})
	if err != nil {
		t.Fatalf("HandleCheckpoint error: %v", err)
	}
	// MapOperator (via BaseOperator) returns nil state.
	if state != nil {
		t.Errorf("expected nil state for non-timer operator, got %v", state)
	}
}
