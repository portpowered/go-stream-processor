package engine

import "sync"

// StateStore provides a key-value state abstraction for nodes. Each node can
// maintain keyed state that is periodically snapshotted during checkpoints
// and restored during recovery.
type StateStore interface {
	// Get retrieves the value for a key. Returns nil if the key does not exist.
	Get(key string) []byte
	// Put stores a value for a key.
	Put(key string, value []byte)
	// Delete removes a key from the store.
	Delete(key string)
	// Snapshot serializes the entire state store contents into a byte slice
	// suitable for persistence. The format is: [keyLen:4][key][valLen:4][val]...
	Snapshot() []byte
	// Restore replaces the store contents with the state from a previously
	// created snapshot. Returns an error if the data is malformed.
	Restore(data []byte) error
}

// InMemoryStateStore is a thread-safe in-memory implementation of StateStore
// using a map[string][]byte.
type InMemoryStateStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewInMemoryStateStore creates a new empty in-memory state store.
func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{
		data: make(map[string][]byte),
	}
}

// Get retrieves the value for a key. Returns nil if not found.
func (s *InMemoryStateStore) Get(key string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil
	}
	// Return a copy to prevent mutation.
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp
}

// Put stores a value for a key.
func (s *InMemoryStateStore) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[key] = cp
}

// Delete removes a key from the store.
func (s *InMemoryStateStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Snapshot serializes the store into a byte slice.
// Format: repeated [keyLen:4 big-endian][key bytes][valLen:4 big-endian][val bytes].
func (s *InMemoryStateStore) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil
	}

	// Calculate total size.
	size := 0
	for k, v := range s.data {
		size += 4 + len(k) + 4 + len(v)
	}

	buf := make([]byte, 0, size)
	for k, v := range s.data {
		buf = appendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = appendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

// Restore replaces the store contents from a snapshot.
func (s *InMemoryStateStore) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string][]byte)
	if len(data) == 0 {
		return nil
	}

	offset := 0
	for offset < len(data) {
		if offset+4 > len(data) {
			return errSnapshotCorrupt
		}
		keyLen := readUint32(data[offset:])
		offset += 4

		if offset+int(keyLen) > len(data) {
			return errSnapshotCorrupt
		}
		key := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		if offset+4 > len(data) {
			return errSnapshotCorrupt
		}
		valLen := readUint32(data[offset:])
		offset += 4

		if offset+int(valLen) > len(data) {
			return errSnapshotCorrupt
		}
		val := make([]byte, valLen)
		copy(val, data[offset:offset+int(valLen)])
		offset += int(valLen)

		s.data[key] = val
	}
	return nil
}

// appendUint32 appends a uint32 in big-endian format.
func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// readUint32 reads a big-endian uint32 from a byte slice.
func readUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// errSnapshotCorrupt is returned when snapshot data is malformed.
var errSnapshotCorrupt = &snapshotError{"corrupt snapshot data"}

type snapshotError struct {
	msg string
}

func (e *snapshotError) Error() string {
	return e.msg
}
