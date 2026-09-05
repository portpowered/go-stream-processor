package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckpointManager_CollectAndSave(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend:   backend,
		KeepCount: 0, // keep all
	})

	// Collect state from two nodes.
	mgr.CollectState(NodeCompletion{NodeID: "A", Epoch: 1, State: []byte("stateA")})
	mgr.CollectState(NodeCompletion{NodeID: "B", Epoch: 1, State: []byte("stateB")})

	// Save the checkpoint.
	if err := mgr.SaveCheckpoint(1); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	// Verify persisted data.
	dataA, err := backend.Load(1, "A")
	if err != nil {
		t.Fatal(err)
	}
	if string(dataA) != "stateA" {
		t.Fatalf("expected 'stateA', got '%s'", dataA)
	}

	dataB, err := backend.Load(1, "B")
	if err != nil {
		t.Fatal(err)
	}
	if string(dataB) != "stateB" {
		t.Fatalf("expected 'stateB', got '%s'", dataB)
	}
}

func TestCheckpointManager_RestoreNodes(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend: backend,
	})

	// Save some state.
	mgr.CollectState(NodeCompletion{NodeID: "A", Epoch: 1, State: []byte("stateA-1")})
	mgr.CollectState(NodeCompletion{NodeID: "B", Epoch: 1, State: []byte("stateB-1")})
	if err := mgr.SaveCheckpoint(1); err != nil {
		t.Fatal(err)
	}

	// Restore from epoch 1.
	states, err := mgr.RestoreNodes(1, []string{"A", "B"})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if string(states["A"]) != "stateA-1" {
		t.Fatalf("expected 'stateA-1', got '%s'", states["A"])
	}
	if string(states["B"]) != "stateB-1" {
		t.Fatalf("expected 'stateB-1', got '%s'", states["B"])
	}
}

func TestCheckpointManager_KeepCountCleanup(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend:   backend,
		KeepCount: 2,
	})

	// Save 4 checkpoints.
	for epoch := uint64(1); epoch <= 4; epoch++ {
		mgr.CollectState(NodeCompletion{NodeID: "A", Epoch: epoch, State: []byte("data")})
		if err := mgr.SaveCheckpoint(epoch); err != nil {
			t.Fatalf("save epoch %d: %v", epoch, err)
		}
	}

	// Only epochs 3 and 4 should remain.
	epochs, err := backend.ListEpochs()
	if err != nil {
		t.Fatal(err)
	}
	if len(epochs) != 2 {
		t.Fatalf("expected 2 epochs, got %d: %v", len(epochs), epochs)
	}
	if epochs[0] != 3 || epochs[1] != 4 {
		t.Fatalf("expected [3,4], got %v", epochs)
	}
}

func TestCheckpointManager_LatestEpoch(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend: backend,
	})

	// No checkpoints yet.
	_, found, err := mgr.LatestEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no latest epoch")
	}

	// Save epoch 3, then epoch 1 (out of order).
	mgr.CollectState(NodeCompletion{NodeID: "A", Epoch: 3, State: []byte("d")})
	if err := mgr.SaveCheckpoint(3); err != nil {
		t.Fatal(err)
	}
	mgr.CollectState(NodeCompletion{NodeID: "A", Epoch: 1, State: []byte("d")})
	if err := mgr.SaveCheckpoint(1); err != nil {
		t.Fatal(err)
	}

	latest, found, err := mgr.LatestEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if !found || latest != 3 {
		t.Fatalf("expected latest=3, got %d (found=%v)", latest, found)
	}
}

func TestCheckpointManager_SaveCheckpointNoData(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend: backend,
	})

	// Saving a checkpoint with no collected state should be a no-op.
	if err := mgr.SaveCheckpoint(99); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestCheckpointManager_WriteAndRestoreRoundTrip tests the full cycle:
// node processes messages, checkpoints state, crash, restore, verify state.
func TestCheckpointManager_WriteAndRestoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend:   backend,
		KeepCount: 3,
	})

	// Simulate a stateful node using InMemoryStateStore.
	store := NewInMemoryStateStore()
	store.Put("counter", []byte("42"))
	store.Put("name", []byte("test-node"))

	// Snapshot the state (this is what HandleCheckpoint would return).
	snap := store.Snapshot()

	// Collect and save.
	mgr.CollectState(NodeCompletion{NodeID: "stateful", Epoch: 1, State: snap})
	if err := mgr.SaveCheckpoint(1); err != nil {
		t.Fatal(err)
	}

	// Simulate crash: create a fresh store and restore from checkpoint.
	store2 := NewInMemoryStateStore()
	states, err := mgr.RestoreNodes(1, []string{"stateful"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store2.Restore(states["stateful"]); err != nil {
		t.Fatal(err)
	}

	// Verify restored state.
	if v := store2.Get("counter"); string(v) != "42" {
		t.Fatalf("expected '42', got '%s'", v)
	}
	if v := store2.Get("name"); string(v) != "test-node" {
		t.Fatalf("expected 'test-node', got '%s'", v)
	}
}

// TestCheckpointManager_RecoveryAfterSimulatedCrash tests restarting a graph
// from a checkpoint after simulated crash.
func TestCheckpointManager_RecoveryAfterSimulatedCrash(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	// --- Phase 1: Run a "pipeline" and checkpoint ---

	mgr1 := NewCheckpointManager(CheckpointManagerConfig{
		Backend:   backend,
		KeepCount: 3,
	})

	// Simulate 3 nodes with state.
	nodeStates := map[string]*InMemoryStateStore{
		"source":    NewInMemoryStateStore(),
		"transform": NewInMemoryStateStore(),
		"sink":      NewInMemoryStateStore(),
	}
	nodeStates["source"].Put("offset", []byte("100"))
	nodeStates["transform"].Put("count", []byte("50"))
	nodeStates["sink"].Put("written", []byte("50"))

	// Create a controller that persists checkpoints.
	var checkpointDone atomic.Int32
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"source", "transform", "sink"},
		SourceIDs: []string{"source"},
		SinkIDs:   []string{"sink"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			checkpointDone.Store(1)
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctrl.Start(ctx)

	// Simulate node completions (as the runtime would produce them).
	for nodeID, store := range nodeStates {
		snap := store.Snapshot()
		completion := NodeCompletion{NodeID: nodeID, Epoch: 1, State: snap}
		mgr1.CollectState(completion)
		ctrl.CompletionChan() <- completion
	}

	// Wait for controller to process.
	for checkpointDone.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// Save checkpoint to disk.
	if err := mgr1.SaveCheckpoint(1); err != nil {
		t.Fatal(err)
	}
	ctrl.Stop()

	// --- Phase 2: Simulate crash and recover ---

	mgr2 := NewCheckpointManager(CheckpointManagerConfig{
		Backend: backend,
	})

	latest, found, err := mgr2.LatestEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if !found || latest != 1 {
		t.Fatalf("expected latest epoch 1, got %d (found=%v)", latest, found)
	}

	states, err := mgr2.RestoreNodes(latest, []string{"source", "transform", "sink"})
	if err != nil {
		t.Fatal(err)
	}

	// Verify each node's state was recovered.
	for nodeID, expectedStore := range nodeStates {
		recovered := NewInMemoryStateStore()
		if err := recovered.Restore(states[nodeID]); err != nil {
			t.Fatalf("restore %s: %v", nodeID, err)
		}
		// Check all keys match.
		for _, key := range []string{"offset", "count", "written"} {
			expected := expectedStore.Get(key)
			actual := recovered.Get(key)
			if expected == nil && actual == nil {
				continue
			}
			if string(expected) != string(actual) {
				t.Fatalf("node %s key %s: expected '%s', got '%s'", nodeID, key, expected, actual)
			}
		}
	}
}
