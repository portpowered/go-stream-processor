package engine

import (
	"fmt"
	"sort"
	"sync"
)

// CheckpointManager orchestrates state serialization across all nodes using a
// pluggable CheckpointBackend. It integrates with the Controller's
// OnCheckpointComplete callback to persist state and handles cleanup of old
// checkpoints.
type CheckpointManager struct {
	mu sync.Mutex

	backend   CheckpointBackend
	keepCount int // number of recent checkpoints to retain (0 = keep all)

	// nodeStates holds in-memory state snapshots collected from NodeCompletion
	// reports, indexed by epoch then nodeID.
	nodeStates map[uint64]map[string][]byte
}

// CheckpointManagerConfig configures a CheckpointManager.
type CheckpointManagerConfig struct {
	// Backend is the persistence layer for checkpoint data.
	Backend CheckpointBackend
	// KeepCount is the number of recent checkpoints to retain. Older
	// checkpoints are automatically deleted. 0 means keep all.
	KeepCount int
}

// NewCheckpointManager creates a new CheckpointManager with the given config.
func NewCheckpointManager(cfg CheckpointManagerConfig) *CheckpointManager {
	return &CheckpointManager{
		backend:    cfg.Backend,
		keepCount:  cfg.KeepCount,
		nodeStates: make(map[uint64]map[string][]byte),
	}
}

// CollectState records a node's state snapshot from a checkpoint completion.
// This should be called for each NodeCompletion received by the controller.
func (m *CheckpointManager) CollectState(completion NodeCompletion) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nodeStates[completion.Epoch] == nil {
		m.nodeStates[completion.Epoch] = make(map[string][]byte)
	}
	m.nodeStates[completion.Epoch][completion.NodeID] = completion.State
}

// SaveCheckpoint persists all collected node states for the given epoch to the
// backend, then cleans up old checkpoints if KeepCount is configured. This is
// intended to be called from the Controller's OnCheckpointComplete callback.
func (m *CheckpointManager) SaveCheckpoint(epoch uint64) error {
	m.mu.Lock()
	states, ok := m.nodeStates[epoch]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	// Copy the states so we can release the lock during I/O.
	statesCopy := make(map[string][]byte, len(states))
	for k, v := range states {
		statesCopy[k] = v
	}
	m.mu.Unlock()

	// Persist each node's state.
	for nodeID, data := range statesCopy {
		if err := m.backend.Save(epoch, nodeID, data); err != nil {
			return fmt.Errorf("save state for node %s epoch %d: %w", nodeID, epoch, err)
		}
	}

	// Clean up old checkpoints.
	if m.keepCount > 0 {
		if err := m.cleanupOldCheckpoints(); err != nil {
			return fmt.Errorf("cleanup old checkpoints: %w", err)
		}
	}

	// Clean up in-memory state for this epoch (it's persisted now).
	m.mu.Lock()
	delete(m.nodeStates, epoch)
	m.mu.Unlock()

	return nil
}

// RestoreNodes loads persisted state for the given nodes at the specified epoch.
// Returns a map of nodeID -> serialized state bytes.
func (m *CheckpointManager) RestoreNodes(epoch uint64, nodeIDs []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		data, err := m.backend.Load(epoch, nodeID)
		if err != nil {
			return nil, fmt.Errorf("load state for node %s epoch %d: %w", nodeID, epoch, err)
		}
		if data != nil {
			result[nodeID] = data
		}
	}
	return result, nil
}

// LatestEpoch returns the most recent persisted checkpoint epoch, or 0 and
// false if no checkpoints exist.
func (m *CheckpointManager) LatestEpoch() (uint64, bool, error) {
	epochs, err := m.backend.ListEpochs()
	if err != nil {
		return 0, false, err
	}
	if len(epochs) == 0 {
		return 0, false, nil
	}
	return epochs[len(epochs)-1], true, nil
}

// cleanupOldCheckpoints removes checkpoints older than the KeepCount most recent.
func (m *CheckpointManager) cleanupOldCheckpoints() error {
	epochs, err := m.backend.ListEpochs()
	if err != nil {
		return err
	}

	if len(epochs) <= m.keepCount {
		return nil
	}

	// Sort ascending (should already be, but be safe).
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })

	// Delete everything except the last keepCount epochs.
	toDelete := epochs[:len(epochs)-m.keepCount]
	for _, epoch := range toDelete {
		if err := m.backend.Delete(epoch); err != nil {
			return fmt.Errorf("delete epoch %d: %w", epoch, err)
		}
	}

	return nil
}
