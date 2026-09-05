package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// CheckpointBackend provides pluggable persistence for checkpoint state data.
// Implementations handle the physical storage of serialized node state snapshots.
type CheckpointBackend interface {
	// Save persists the serialized state for a node at a given epoch.
	Save(epoch uint64, nodeID string, data []byte) error
	// Load retrieves the serialized state for a node at a given epoch.
	// Returns nil, nil if no data exists for the given epoch/node.
	Load(epoch uint64, nodeID string) ([]byte, error)
	// ListEpochs returns all epochs that have persisted data, sorted ascending.
	ListEpochs() ([]uint64, error)
	// Delete removes all persisted data for a given epoch.
	Delete(epoch uint64) error
}

// LocalDiskBackend implements CheckpointBackend by writing state files to a
// local directory. Each epoch gets a subdirectory, and each node's state is
// stored as a file within that directory. Writes use atomic rename (write to
// temp file, then rename) to prevent partial writes.
//
// Directory structure:
//
//	<baseDir>/
//	  <epoch>/
//	    <nodeID>.state
type LocalDiskBackend struct {
	mu      sync.Mutex
	baseDir string
}

// NewLocalDiskBackend creates a new LocalDiskBackend that writes checkpoint
// data to the given directory. The directory is created if it does not exist.
func NewLocalDiskBackend(baseDir string) (*LocalDiskBackend, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory: %w", err)
	}
	return &LocalDiskBackend{baseDir: baseDir}, nil
}

// Save persists node state to disk using atomic rename.
func (b *LocalDiskBackend) Save(epoch uint64, nodeID string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	epochDir := filepath.Join(b.baseDir, strconv.FormatUint(epoch, 10))
	if err := os.MkdirAll(epochDir, 0o755); err != nil {
		return fmt.Errorf("create epoch directory: %w", err)
	}

	target := filepath.Join(epochDir, nodeID+".state")

	// Write to temp file first, then atomic rename.
	tmp, err := os.CreateTemp(epochDir, nodeID+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename state file: %w", err)
	}

	return nil
}

// Load reads the persisted state for a node at a given epoch.
func (b *LocalDiskBackend) Load(epoch uint64, nodeID string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	target := filepath.Join(b.baseDir, strconv.FormatUint(epoch, 10), nodeID+".state")
	data, err := os.ReadFile(target)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	return data, nil
}

// ListEpochs returns all persisted epoch numbers, sorted ascending.
func (b *LocalDiskBackend) ListEpochs() ([]uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	entries, err := os.ReadDir(b.baseDir)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint directory: %w", err)
	}

	var epochs []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		epoch, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil {
			continue // skip non-epoch directories
		}
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs, nil
}

// Delete removes all persisted data for a given epoch.
func (b *LocalDiskBackend) Delete(epoch uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	epochDir := filepath.Join(b.baseDir, strconv.FormatUint(epoch, 10))
	if err := os.RemoveAll(epochDir); err != nil {
		return fmt.Errorf("delete epoch directory: %w", err)
	}
	return nil
}
