package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// DebounceOperator suppresses repeated signals within a configurable window.
// When a message arrives for a key, a timer is (re-)set for that key. Only
// after the window expires with no new message for that key is the last message
// emitted. This implements hysteresis / debouncing behavior.
//
// DebounceOperator uses timers (not goroutine sleeps) for non-blocking behavior
// and checkpoints its state correctly.
type DebounceOperator struct {
	BaseOperator

	// Window is the debounce window duration. A message is emitted only
	// after no new message arrives for this key within the window.
	Window time.Duration

	mu      sync.Mutex
	pending map[string]debounceEntry
	counter uint64
}

type debounceEntry struct {
	Msg       DataMessage
	TimerName string
}

// NewDebounceOperator creates a DebounceOperator with the given window.
func NewDebounceOperator(window time.Duration) *DebounceOperator {
	return &DebounceOperator{
		Window:  window,
		pending: make(map[string]debounceEntry),
	}
}

// ProcessMessage resets the debounce timer for the message's key.
func (d *DebounceOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	d.mu.Lock()
	d.counter++
	timerName := fmt.Sprintf("debounce-%d", d.counter)
	d.pending[msg.Key] = debounceEntry{Msg: msg, TimerName: timerName}
	d.mu.Unlock()

	collector.RegisterTimer(timerName, time.Now().Add(d.Window))
	return nil
}

// HandleTimer emits the debounced message if the timer is still the active one for its key.
func (d *DebounceOperator) HandleTimer(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
	d.mu.Lock()
	// Find which key this timer belongs to and check if it's still current.
	var foundKey string
	var foundMsg DataMessage
	var found bool
	for key, entry := range d.pending {
		if entry.TimerName == name {
			foundKey = key
			foundMsg = entry.Msg
			found = true
			break
		}
	}
	if found {
		delete(d.pending, foundKey)
	}
	d.mu.Unlock()

	if found {
		return collector.Emit(foundMsg)
	}
	return nil
}

// HandleCheckpoint snapshots the pending entries and counter.
// Format: [counter:8][count:4] then for each: [keyLen:4][key][timerNameLen:4][timerName]
// Note: message payloads (DataMessage.Value) are type `any` and not serialized.
// Timer registrations are checkpointed by the runtime's timerManager.
func (d *DebounceOperator) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.pending) == 0 {
		// Still encode counter for continuity.
		buf := make([]byte, 8+4)
		binary.BigEndian.PutUint64(buf[:8], d.counter)
		binary.BigEndian.PutUint32(buf[8:12], 0)
		return buf, nil
	}

	size := 8 + 4
	for key, entry := range d.pending {
		size += 4 + len(key) + 4 + len(entry.TimerName)
	}
	buf := make([]byte, 0, size)

	counterBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(counterBytes, d.counter)
	buf = append(buf, counterBytes...)

	buf = appendUint32(buf, uint32(len(d.pending)))
	for key, entry := range d.pending {
		buf = appendUint32(buf, uint32(len(key)))
		buf = append(buf, key...)
		buf = appendUint32(buf, uint32(len(entry.TimerName)))
		buf = append(buf, entry.TimerName...)
	}
	return buf, nil
}

// DeduplicateOperator drops messages with duplicate keys within a configurable
// time window. Uses timers to expire old keys so that messages after the window
// are not considered duplicates.
//
// DeduplicateOperator uses timers (not goroutine sleeps) for non-blocking behavior
// and checkpoints its state correctly.
type DeduplicateOperator struct {
	BaseOperator

	// Window is the deduplication window. Messages with the same key within
	// this window are considered duplicates and dropped.
	Window time.Duration

	// KeyFn extracts the deduplication key from a message. If nil, the
	// message's Key field is used.
	KeyFn func(DataMessage) string

	mu      sync.Mutex
	seen    map[string]time.Time // key → expiry time
	counter uint64
}

// NewDeduplicateOperator creates a DeduplicateOperator with the given window.
func NewDeduplicateOperator(window time.Duration) *DeduplicateOperator {
	return &DeduplicateOperator{
		Window: window,
		seen:   make(map[string]time.Time),
	}
}

func (dd *DeduplicateOperator) dedupKey(msg DataMessage) string {
	if dd.KeyFn != nil {
		return dd.KeyFn(msg)
	}
	return msg.Key
}

// ProcessMessage emits the message only if its key has not been seen within the window.
func (dd *DeduplicateOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	key := dd.dedupKey(msg)
	now := time.Now()
	expiry := now.Add(dd.Window)

	dd.mu.Lock()
	if existing, ok := dd.seen[key]; ok && existing.After(now) {
		// Duplicate within window — drop.
		dd.mu.Unlock()
		return nil
	}
	dd.seen[key] = expiry
	dd.counter++
	timerName := fmt.Sprintf("dedup-expire-%d", dd.counter)
	dd.mu.Unlock()

	collector.RegisterTimer(timerName, expiry)
	return collector.Emit(msg)
}

// HandleTimer removes expired keys from the seen set.
func (dd *DeduplicateOperator) HandleTimer(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	// Clean up any keys that have expired by the timer's fire time.
	for key, expiry := range dd.seen {
		if !expiry.After(fireAt) {
			delete(dd.seen, key)
		}
	}
	return nil
}

// HandleCheckpoint snapshots the seen keys and their expiry times.
// Format: [counter:8][count:4] then for each: [keyLen:4][key][expiryUnixNano:8]
func (dd *DeduplicateOperator) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	size := 8 + 4
	for key := range dd.seen {
		size += 4 + len(key) + 8
	}
	buf := make([]byte, 0, size)

	counterBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(counterBytes, dd.counter)
	buf = append(buf, counterBytes...)

	buf = appendUint32(buf, uint32(len(dd.seen)))
	for key, expiry := range dd.seen {
		buf = appendUint32(buf, uint32(len(key)))
		buf = append(buf, key...)
		ns := make([]byte, 8)
		binary.BigEndian.PutUint64(ns, uint64(expiry.UnixNano()))
		buf = append(buf, ns...)
	}
	return buf, nil
}
