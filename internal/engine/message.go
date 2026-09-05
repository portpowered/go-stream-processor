// Package esp provides an embedded stream processor library inspired by
// Arroyo's architecture, featuring in-band signal/data message model,
// barrier-based checkpointing (Chandy-Lamport), backpressure via bounded
// buffers, and clean separation of control and data planes.
package engine

import "time"

// MessageType identifies whether a Message carries data or a signal.
type MessageType int

const (
	// MessageTypeData indicates the message carries a user data payload.
	MessageTypeData MessageType = iota
	// MessageTypeSignal indicates the message carries an in-band signal.
	MessageTypeSignal
)

// Message is the fundamental unit that flows through the stream processor graph.
// A Message is either a data message carrying a user payload, or a signal
// message carrying control information (barriers, watermarks, stop).
// Exactly one of Data or Signal will be non-nil.
type Message struct {
	// Type indicates whether this message carries data or a signal.
	Type MessageType
	// Data holds the user payload. Non-nil only when Type == MessageTypeData.
	Data *DataMessage
	// Signal holds the in-band signal. Non-nil only when Type == MessageTypeSignal.
	Signal *SignalMessage
}

// NewDataMessage creates a new Message wrapping a data payload.
func NewDataMessage(key string, value any, eventTime time.Time) Message {
	return Message{
		Type: MessageTypeData,
		Data: &DataMessage{
			Key:       key,
			Value:     value,
			EventTime: eventTime,
		},
	}
}

// NewSignalMessage creates a new Message wrapping a signal.
func NewSignalMessage(signal SignalMessage) Message {
	return Message{
		Type:   MessageTypeSignal,
		Signal: &signal,
	}
}

// IsData returns true if this message carries a data payload.
func (m Message) IsData() bool {
	return m.Type == MessageTypeData
}

// IsSignal returns true if this message carries an in-band signal.
func (m Message) IsSignal() bool {
	return m.Type == MessageTypeSignal
}

// DataMessage carries a user-defined payload through the graph.
type DataMessage struct {
	// Key is an optional partitioning key for keyed state operations.
	Key string
	// Value is the user payload. The stream processor is payload-agnostic.
	Value any
	// EventTime is the event timestamp associated with this data.
	EventTime time.Time
}

// SignalType identifies the kind of in-band signal.
type SignalType int

const (
	// SignalTypeBarrier is a checkpoint barrier signal (Chandy-Lamport).
	SignalTypeBarrier SignalType = iota
	// SignalTypeWatermark is an event-time watermark signal.
	SignalTypeWatermark
	// SignalTypeStop indicates the source has finished producing data.
	SignalTypeStop
)

// SignalMessage is an in-band signal that flows through the graph alongside
// data messages. Signals are used for checkpoint barriers, watermarks, and
// stop indicators. Exactly one of Barrier, Watermark fields will be set
// (for their respective signal types), or neither for Stop signals.
type SignalMessage struct {
	// SignalType identifies which kind of signal this is.
	SignalType SignalType
	// Barrier holds checkpoint barrier details. Non-nil only when SignalType == SignalTypeBarrier.
	Barrier *BarrierSignal
	// Watermark holds watermark details. Non-nil only when SignalType == SignalTypeWatermark.
	Watermark *WatermarkSignal
}

// BarrierSignal carries checkpoint barrier information for the Chandy-Lamport
// snapshotting algorithm. Barriers flow in-band with data and trigger state
// snapshots at each node.
type BarrierSignal struct {
	// Epoch is the monotonically increasing checkpoint epoch number.
	Epoch uint64
	// MinEpoch is the minimum epoch that must be retained for recovery.
	MinEpoch uint64
	// Timestamp is when this barrier was created.
	Timestamp time.Time
	// ThenStop indicates the pipeline should stop after this checkpoint completes.
	ThenStop bool
}

// WatermarkSignal carries event-time progress information. Watermarks flow
// in-band and allow downstream nodes to reason about event-time completeness.
type WatermarkSignal struct {
	// EventTime is the watermark timestamp. All events with timestamps
	// strictly less than this value are guaranteed to have been processed.
	EventTime time.Time
	// Idle indicates the source has no new data but is still alive.
	Idle bool
}

// NewBarrierSignal creates a SignalMessage containing a checkpoint barrier.
func NewBarrierSignal(epoch, minEpoch uint64, thenStop bool) SignalMessage {
	return SignalMessage{
		SignalType: SignalTypeBarrier,
		Barrier: &BarrierSignal{
			Epoch:     epoch,
			MinEpoch:  minEpoch,
			Timestamp: time.Now(),
			ThenStop:  thenStop,
		},
	}
}

// NewWatermarkSignal creates a SignalMessage containing an event-time watermark.
func NewWatermarkSignal(eventTime time.Time) SignalMessage {
	return SignalMessage{
		SignalType: SignalTypeWatermark,
		Watermark: &WatermarkSignal{
			EventTime: eventTime,
		},
	}
}

// NewIdleWatermarkSignal creates a SignalMessage indicating the source is idle.
func NewIdleWatermarkSignal() SignalMessage {
	return SignalMessage{
		SignalType: SignalTypeWatermark,
		Watermark: &WatermarkSignal{
			Idle: true,
		},
	}
}

// NewStopSignal creates a SignalMessage indicating the source has finished.
func NewStopSignal() SignalMessage {
	return SignalMessage{
		SignalType: SignalTypeStop,
	}
}
