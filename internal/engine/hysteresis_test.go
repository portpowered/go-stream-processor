package engine

import (
	"context"
	"testing"
	"time"
)

// --- DebounceOperator tests ---

func TestDebounceOperator_RapidTogglingProducesOneOutput(t *testing.T) {
	// Debounce window of 50ms. Rapid messages for the same key within the
	// window should result in only the last message being emitted.
	op := NewDebounceOperator(50 * time.Millisecond)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Send 5 rapid messages for the same key — each resets the debounce timer.
	for i := 0; i < 5; i++ {
		msg := DataMessage{Key: "toggle", Value: i, EventTime: time.Now()}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	// At this point, 5 timers have been registered but only the last one
	// should be "active" (i.e., its timer name matches the pending entry).
	// Simulate firing all timers — only the last should produce output.
	timers := collector.timers
	if len(timers) != 5 {
		t.Fatalf("expected 5 timer registrations, got %d", len(timers))
	}

	for _, timer := range timers {
		if err := op.HandleTimer(ctx, timer.Name, timer.FireAt, collector); err != nil {
			t.Fatalf("HandleTimer error: %v", err)
		}
	}

	msgs := collector.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 debounced message, got %d", len(msgs))
	}

	// Should be the last value (4).
	if msgs[0].Value != 4 {
		t.Errorf("expected value 4, got %v", msgs[0].Value)
	}
	if msgs[0].Key != "toggle" {
		t.Errorf("expected key 'toggle', got %q", msgs[0].Key)
	}
}

func TestDebounceOperator_DifferentKeysDebounceIndependently(t *testing.T) {
	op := NewDebounceOperator(50 * time.Millisecond)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Send messages for two different keys.
	msgs := []DataMessage{
		{Key: "a", Value: "a1"},
		{Key: "b", Value: "b1"},
		{Key: "a", Value: "a2"}, // replaces a1
		{Key: "b", Value: "b2"}, // replaces b1
	}
	for _, msg := range msgs {
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	// Fire all timers.
	for _, timer := range collector.timers {
		if err := op.HandleTimer(ctx, timer.Name, timer.FireAt, collector); err != nil {
			t.Fatalf("HandleTimer error: %v", err)
		}
	}

	emitted := collector.Messages()
	if len(emitted) != 2 {
		t.Fatalf("expected 2 debounced messages (one per key), got %d", len(emitted))
	}

	// Collect emitted values by key.
	byKey := make(map[string]any)
	for _, m := range emitted {
		byKey[m.Key] = m.Value
	}

	if byKey["a"] != "a2" {
		t.Errorf("expected key 'a' value 'a2', got %v", byKey["a"])
	}
	if byKey["b"] != "b2" {
		t.Errorf("expected key 'b' value 'b2', got %v", byKey["b"])
	}
}

func TestDebounceOperator_SingleMessageEmitsAfterWindow(t *testing.T) {
	op := NewDebounceOperator(50 * time.Millisecond)
	collector := newCollectingCollector()
	ctx := context.Background()

	msg := DataMessage{Key: "k", Value: "only", EventTime: time.Now()}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	// Fire the timer.
	if len(collector.timers) != 1 {
		t.Fatalf("expected 1 timer, got %d", len(collector.timers))
	}
	if err := op.HandleTimer(ctx, collector.timers[0].Name, collector.timers[0].FireAt, collector); err != nil {
		t.Fatalf("HandleTimer error: %v", err)
	}

	emitted := collector.Messages()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 message, got %d", len(emitted))
	}
	if emitted[0].Value != "only" {
		t.Errorf("expected 'only', got %v", emitted[0].Value)
	}
}

func TestDebounceOperator_CheckpointState(t *testing.T) {
	op := NewDebounceOperator(50 * time.Millisecond)
	ctx := context.Background()
	collector := newCollectingCollector()

	// Add some pending messages.
	msg := DataMessage{Key: "k1", Value: "v1"}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	// Checkpoint.
	state, err := op.HandleCheckpoint(ctx, BarrierSignal{Epoch: 1})
	if err != nil {
		t.Fatalf("HandleCheckpoint error: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil checkpoint state")
	}
	if len(state) < 12 { // 8 bytes counter + 4 bytes count minimum
		t.Fatalf("checkpoint state too short: %d bytes", len(state))
	}
}

// --- DeduplicateOperator tests ---

func TestDeduplicateOperator_DropsDuplicatesWithinWindow(t *testing.T) {
	op := NewDeduplicateOperator(100 * time.Millisecond)
	collector := newCollectingCollector()
	ctx := context.Background()

	// Send 3 messages with the same key — only the first should pass.
	for i := 0; i < 3; i++ {
		msg := DataMessage{Key: "dup", Value: i, EventTime: time.Now()}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	emitted := collector.Messages()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 message (duplicates dropped), got %d", len(emitted))
	}
	if emitted[0].Value != 0 {
		t.Errorf("expected first value 0, got %v", emitted[0].Value)
	}
}

func TestDeduplicateOperator_MessagesAfterWindowAreNotDuplicates(t *testing.T) {
	op := NewDeduplicateOperator(50 * time.Millisecond)
	collector := newCollectingCollector()
	ctx := context.Background()

	// First message passes.
	msg1 := DataMessage{Key: "k", Value: "first", EventTime: time.Now()}
	if err := op.ProcessMessage(ctx, msg1, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	// Fire the expiry timer to clear the key.
	if len(collector.timers) != 1 {
		t.Fatalf("expected 1 timer, got %d", len(collector.timers))
	}
	if err := op.HandleTimer(ctx, collector.timers[0].Name, collector.timers[0].FireAt, collector); err != nil {
		t.Fatalf("HandleTimer error: %v", err)
	}

	// Same key after expiry — should pass again.
	msg2 := DataMessage{Key: "k", Value: "second", EventTime: time.Now()}
	if err := op.ProcessMessage(ctx, msg2, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	emitted := collector.Messages()
	if len(emitted) != 2 {
		t.Fatalf("expected 2 messages (after window expiry), got %d", len(emitted))
	}
	if emitted[0].Value != "first" {
		t.Errorf("expected 'first', got %v", emitted[0].Value)
	}
	if emitted[1].Value != "second" {
		t.Errorf("expected 'second', got %v", emitted[1].Value)
	}
}

func TestDeduplicateOperator_DifferentKeysPassThrough(t *testing.T) {
	op := NewDeduplicateOperator(100 * time.Millisecond)
	collector := newCollectingCollector()
	ctx := context.Background()

	keys := []string{"a", "b", "c", "a", "b", "c"}
	for i, key := range keys {
		msg := DataMessage{Key: key, Value: i, EventTime: time.Now()}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	emitted := collector.Messages()
	// Only first occurrence of each key should pass.
	if len(emitted) != 3 {
		t.Fatalf("expected 3 messages (unique keys), got %d", len(emitted))
	}

	expectedValues := []int{0, 1, 2}
	for i, msg := range emitted {
		if msg.Value != expectedValues[i] {
			t.Errorf("message %d: expected value %d, got %v", i, expectedValues[i], msg.Value)
		}
	}
}

func TestDeduplicateOperator_CustomKeyFn(t *testing.T) {
	op := NewDeduplicateOperator(100 * time.Millisecond)
	op.KeyFn = func(msg DataMessage) string {
		// Deduplicate by value content instead of key.
		if v, ok := msg.Value.(string); ok {
			return v
		}
		return msg.Key
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	// Different keys but same value — should deduplicate.
	msgs := []DataMessage{
		{Key: "1", Value: "same"},
		{Key: "2", Value: "same"},  // dup by value
		{Key: "3", Value: "other"}, // different value, passes
	}

	for _, msg := range msgs {
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	emitted := collector.Messages()
	if len(emitted) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(emitted))
	}
	if emitted[0].Key != "1" {
		t.Errorf("expected first key '1', got %q", emitted[0].Key)
	}
	if emitted[1].Key != "3" {
		t.Errorf("expected second key '3', got %q", emitted[1].Key)
	}
}

func TestDeduplicateOperator_CheckpointState(t *testing.T) {
	op := NewDeduplicateOperator(100 * time.Millisecond)
	ctx := context.Background()
	collector := newCollectingCollector()

	// Process a message so there's state to checkpoint.
	msg := DataMessage{Key: "k1", Value: "v1"}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	state, err := op.HandleCheckpoint(ctx, BarrierSignal{Epoch: 1})
	if err != nil {
		t.Fatalf("HandleCheckpoint error: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil checkpoint state")
	}
	if len(state) < 12 { // 8 bytes counter + 4 bytes count minimum
		t.Fatalf("checkpoint state too short: %d bytes", len(state))
	}
}

// --- Verify both operators implement TimerOperator ---

func TestDebounceOperator_ImplementsTimerOperator(t *testing.T) {
	op := NewDebounceOperator(50 * time.Millisecond)
	var _ TimerOperator = op
}

func TestDeduplicateOperator_ImplementsTimerOperator(t *testing.T) {
	op := NewDeduplicateOperator(50 * time.Millisecond)
	var _ TimerOperator = op
}
