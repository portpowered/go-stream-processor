package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TimerOperator extends Operator with timer callback support.
// Operators that register timers via Collector.RegisterTimer should
// implement this interface to receive callbacks when timers fire.
type TimerOperator interface {
	Operator

	// HandleTimer is called when a registered timer fires. The name parameter
	// matches the name passed to Collector.RegisterTimer, and fireAt is the
	// scheduled fire time.
	HandleTimer(ctx context.Context, name string, fireAt time.Time, collector Collector) error
}

// TimerHandler is an optional interface that Node implementations can provide
// to receive timer callbacks from the runtime.
type TimerHandler interface {
	HandleTimer(ctx context.Context, name string, fireAt time.Time, emit Emitter) error
}

// timerManagerProvider is implemented by nodes that manage timers.
type timerManagerProvider interface {
	getTimerManager() *timerManager
}

// timerEntry represents a registered timer.
type timerEntry struct {
	Name   string
	FireAt time.Time
}

// timerManager manages registered timers for a node. It maintains timers
// sorted by fire time and supports checkpoint/restore.
type timerManager struct {
	mu     sync.Mutex
	timers []timerEntry
}

func newTimerManager() *timerManager {
	return &timerManager{}
}

// Register adds a new timer. Timers are maintained sorted by fire time.
func (tm *timerManager) Register(name string, fireAt time.Time) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	entry := timerEntry{Name: name, FireAt: fireAt}
	idx := sort.Search(len(tm.timers), func(i int) bool {
		return tm.timers[i].FireAt.After(fireAt)
	})
	tm.timers = append(tm.timers, timerEntry{})
	copy(tm.timers[idx+1:], tm.timers[idx:])
	tm.timers[idx] = entry
}

// PopExpired removes and returns all timers with FireAt <= now.
func (tm *timerManager) PopExpired(now time.Time) []timerEntry {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(tm.timers) == 0 {
		return nil
	}

	idx := sort.Search(len(tm.timers), func(i int) bool {
		return tm.timers[i].FireAt.After(now)
	})
	if idx == 0 {
		return nil
	}

	expired := make([]timerEntry, idx)
	copy(expired, tm.timers[:idx])
	tm.timers = tm.timers[idx:]
	return expired
}

// Peek returns the next timer to fire, or false if no timers exist.
func (tm *timerManager) Peek() (timerEntry, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if len(tm.timers) == 0 {
		return timerEntry{}, false
	}
	return tm.timers[0], true
}

// Len returns the number of pending timers.
func (tm *timerManager) Len() int {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return len(tm.timers)
}

// Snapshot serializes all pending timers.
// Format: [count:4] then for each: [nameLen:4][name][fireAtUnixNano:8]
func (tm *timerManager) Snapshot() []byte {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(tm.timers) == 0 {
		return nil
	}

	size := 4
	for _, t := range tm.timers {
		size += 4 + len(t.Name) + 8
	}

	buf := make([]byte, 0, size)
	buf = appendUint32(buf, uint32(len(tm.timers)))
	for _, t := range tm.timers {
		buf = appendUint32(buf, uint32(len(t.Name)))
		buf = append(buf, t.Name...)
		ns := make([]byte, 8)
		binary.BigEndian.PutUint64(ns, uint64(t.FireAt.UnixNano()))
		buf = append(buf, ns...)
	}
	return buf
}

// Restore replaces all timers from a snapshot.
func (tm *timerManager) Restore(data []byte) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.timers = nil
	if len(data) == 0 {
		return nil
	}
	if len(data) < 4 {
		return errSnapshotCorrupt
	}

	count := readUint32(data[:4])
	offset := 4
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(data) {
			return errSnapshotCorrupt
		}
		nameLen := readUint32(data[offset:])
		offset += 4

		if offset+int(nameLen) > len(data) {
			return errSnapshotCorrupt
		}
		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		if offset+8 > len(data) {
			return errSnapshotCorrupt
		}
		ns := binary.BigEndian.Uint64(data[offset:])
		offset += 8

		tm.timers = append(tm.timers, timerEntry{
			Name:   name,
			FireAt: time.Unix(0, int64(ns)),
		})
	}
	return nil
}

// encodeTimerCheckpoint combines operator state and timer state into a single
// byte slice. Format: [timerStateLen:4][timerState][operatorState]
func encodeTimerCheckpoint(opState, timerState []byte) []byte {
	buf := make([]byte, 4+len(timerState)+len(opState))
	buf[0] = byte(uint32(len(timerState)) >> 24)
	buf[1] = byte(uint32(len(timerState)) >> 16)
	buf[2] = byte(uint32(len(timerState)) >> 8)
	buf[3] = byte(uint32(len(timerState)))
	copy(buf[4:], timerState)
	copy(buf[4+len(timerState):], opState)
	return buf
}

// decodeTimerCheckpoint splits a combined checkpoint into operator and timer state.
func decodeTimerCheckpoint(data []byte) (opState, timerState []byte, err error) {
	if len(data) < 4 {
		return nil, nil, errSnapshotCorrupt
	}
	timerLen := readUint32(data[:4])
	if 4+int(timerLen) > len(data) {
		return nil, nil, errSnapshotCorrupt
	}
	timerState = data[4 : 4+timerLen]
	opState = data[4+timerLen:]
	if len(timerState) == 0 {
		timerState = nil
	}
	if len(opState) == 0 {
		opState = nil
	}
	return opState, timerState, nil
}

// --- SleepOperator ---

// SleepOperator delays messages by a configurable duration using timers.
// Each incoming message is buffered and re-emitted after the delay expires.
type SleepOperator struct {
	BaseOperator

	// Delay is the duration to delay each message.
	Delay time.Duration

	mu      sync.Mutex
	pending map[string]DataMessage
	counter uint64
}

// NewSleepOperator creates a SleepOperator with the given delay duration.
func NewSleepOperator(delay time.Duration) *SleepOperator {
	return &SleepOperator{
		Delay:   delay,
		pending: make(map[string]DataMessage),
	}
}

// ProcessMessage stores the message and registers a timer for delayed emission.
func (s *SleepOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	s.mu.Lock()
	s.counter++
	name := fmt.Sprintf("sleep-%d", s.counter)
	s.pending[name] = msg
	s.mu.Unlock()

	collector.RegisterTimer(name, time.Now().Add(s.Delay))
	return nil
}

// HandleTimer emits the previously buffered message when the timer fires.
func (s *SleepOperator) HandleTimer(ctx context.Context, name string, fireAt time.Time, collector Collector) error {
	s.mu.Lock()
	msg, ok := s.pending[name]
	if ok {
		delete(s.pending, name)
	}
	s.mu.Unlock()

	if ok {
		return collector.Emit(msg)
	}
	return nil
}

// HandleCheckpoint snapshots the pending message count and timer names.
// Note: message payloads (DataMessage.Value) are not serialized since they
// are of type `any`. Timer registrations (name + fireAt) are checkpointed
// by the runtime's timerManager.
func (s *SleepOperator) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return nil, nil
	}

	// Serialize pending timer names and counter.
	// Format: [counter:8][count:4] then for each: [nameLen:4][name]
	size := 8 + 4
	for name := range s.pending {
		size += 4 + len(name)
	}
	buf := make([]byte, 0, size)

	counterBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(counterBytes, s.counter)
	buf = append(buf, counterBytes...)

	buf = appendUint32(buf, uint32(len(s.pending)))
	for name := range s.pending {
		buf = appendUint32(buf, uint32(len(name)))
		buf = append(buf, name...)
	}
	return buf, nil
}
