package engine

import (
	"testing"
	"time"
)

func TestNewDataMessage(t *testing.T) {
	now := time.Now()
	msg := NewDataMessage("key1", "hello", now)

	if !msg.IsData() {
		t.Fatal("expected IsData() to be true")
	}
	if msg.IsSignal() {
		t.Fatal("expected IsSignal() to be false")
	}
	if msg.Type != MessageTypeData {
		t.Fatalf("expected MessageTypeData, got %d", msg.Type)
	}
	if msg.Data == nil {
		t.Fatal("expected Data to be non-nil")
	}
	if msg.Signal != nil {
		t.Fatal("expected Signal to be nil")
	}
	if msg.Data.Key != "key1" {
		t.Fatalf("expected key 'key1', got %q", msg.Data.Key)
	}
	if msg.Data.Value != "hello" {
		t.Fatalf("expected value 'hello', got %v", msg.Data.Value)
	}
	if !msg.Data.EventTime.Equal(now) {
		t.Fatalf("expected event time %v, got %v", now, msg.Data.EventTime)
	}
}

func TestNewSignalMessage_Barrier(t *testing.T) {
	sig := NewBarrierSignal(5, 3, false)
	msg := NewSignalMessage(sig)

	if msg.IsData() {
		t.Fatal("expected IsData() to be false")
	}
	if !msg.IsSignal() {
		t.Fatal("expected IsSignal() to be true")
	}
	if msg.Signal == nil {
		t.Fatal("expected Signal to be non-nil")
	}
	if msg.Signal.SignalType != SignalTypeBarrier {
		t.Fatalf("expected SignalTypeBarrier, got %d", msg.Signal.SignalType)
	}
	if msg.Signal.Barrier == nil {
		t.Fatal("expected Barrier to be non-nil")
	}
	if msg.Signal.Barrier.Epoch != 5 {
		t.Fatalf("expected epoch 5, got %d", msg.Signal.Barrier.Epoch)
	}
	if msg.Signal.Barrier.MinEpoch != 3 {
		t.Fatalf("expected min epoch 3, got %d", msg.Signal.Barrier.MinEpoch)
	}
	if msg.Signal.Barrier.ThenStop {
		t.Fatal("expected ThenStop to be false")
	}
	if msg.Signal.Barrier.Timestamp.IsZero() {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestNewSignalMessage_BarrierThenStop(t *testing.T) {
	sig := NewBarrierSignal(10, 8, true)

	if !sig.Barrier.ThenStop {
		t.Fatal("expected ThenStop to be true")
	}
}

func TestNewSignalMessage_Watermark(t *testing.T) {
	eventTime := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	sig := NewWatermarkSignal(eventTime)
	msg := NewSignalMessage(sig)

	if !msg.IsSignal() {
		t.Fatal("expected IsSignal() to be true")
	}
	if msg.Signal.SignalType != SignalTypeWatermark {
		t.Fatalf("expected SignalTypeWatermark, got %d", msg.Signal.SignalType)
	}
	if msg.Signal.Watermark == nil {
		t.Fatal("expected Watermark to be non-nil")
	}
	if !msg.Signal.Watermark.EventTime.Equal(eventTime) {
		t.Fatalf("expected event time %v, got %v", eventTime, msg.Signal.Watermark.EventTime)
	}
	if msg.Signal.Watermark.Idle {
		t.Fatal("expected Idle to be false")
	}
}

func TestNewSignalMessage_IdleWatermark(t *testing.T) {
	sig := NewIdleWatermarkSignal()

	if sig.SignalType != SignalTypeWatermark {
		t.Fatalf("expected SignalTypeWatermark, got %d", sig.SignalType)
	}
	if !sig.Watermark.Idle {
		t.Fatal("expected Idle to be true")
	}
}

func TestNewSignalMessage_Stop(t *testing.T) {
	sig := NewStopSignal()
	msg := NewSignalMessage(sig)

	if !msg.IsSignal() {
		t.Fatal("expected IsSignal() to be true")
	}
	if msg.Signal.SignalType != SignalTypeStop {
		t.Fatalf("expected SignalTypeStop, got %d", msg.Signal.SignalType)
	}
	if msg.Signal.Barrier != nil {
		t.Fatal("expected Barrier to be nil for stop signal")
	}
	if msg.Signal.Watermark != nil {
		t.Fatal("expected Watermark to be nil for stop signal")
	}
}

func TestMessageTypeAssertions(t *testing.T) {
	tests := []struct {
		name     string
		msg      Message
		isData   bool
		isSignal bool
	}{
		{
			name:     "data message",
			msg:      NewDataMessage("k", 42, time.Now()),
			isData:   true,
			isSignal: false,
		},
		{
			name:     "barrier signal",
			msg:      NewSignalMessage(NewBarrierSignal(1, 0, false)),
			isData:   false,
			isSignal: true,
		},
		{
			name:     "watermark signal",
			msg:      NewSignalMessage(NewWatermarkSignal(time.Now())),
			isData:   false,
			isSignal: true,
		},
		{
			name:     "stop signal",
			msg:      NewSignalMessage(NewStopSignal()),
			isData:   false,
			isSignal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.msg.IsData(); got != tt.isData {
				t.Fatalf("IsData() = %v, want %v", got, tt.isData)
			}
			if got := tt.msg.IsSignal(); got != tt.isSignal {
				t.Fatalf("IsSignal() = %v, want %v", got, tt.isSignal)
			}
		})
	}
}

func TestDataMessagePayloadTypes(t *testing.T) {
	// Verify the Value field can hold various types.
	tests := []struct {
		name  string
		value any
	}{
		{"string", "hello"},
		{"int", 42},
		{"float", 3.14},
		{"slice", []int{1, 2, 3}},
		{"map", map[string]int{"a": 1}},
		{"struct", struct{ X int }{X: 5}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewDataMessage("key", tt.value, time.Now())
			if msg.Data == nil {
				t.Fatal("expected Data to be non-nil")
			}
			// Just verify the message was constructed without panic.
			// Non-comparable types can't use == so we check non-nil.
			if tt.value == nil {
				if msg.Data.Value != nil {
					t.Fatal("expected nil value")
				}
			} else if msg.Data.Value == nil {
				t.Fatal("expected non-nil value")
			}
		})
	}
}
