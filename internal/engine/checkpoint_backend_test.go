package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalDiskBackend_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	// Save state for two nodes at epoch 1.
	if err := backend.Save(1, "nodeA", []byte("stateA")); err != nil {
		t.Fatalf("save nodeA: %v", err)
	}
	if err := backend.Save(1, "nodeB", []byte("stateB")); err != nil {
		t.Fatalf("save nodeB: %v", err)
	}

	// Load them back.
	dataA, err := backend.Load(1, "nodeA")
	if err != nil {
		t.Fatalf("load nodeA: %v", err)
	}
	if string(dataA) != "stateA" {
		t.Fatalf("expected 'stateA', got '%s'", dataA)
	}

	dataB, err := backend.Load(1, "nodeB")
	if err != nil {
		t.Fatalf("load nodeB: %v", err)
	}
	if string(dataB) != "stateB" {
		t.Fatalf("expected 'stateB', got '%s'", dataB)
	}
}

func TestLocalDiskBackend_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	data, err := backend.Load(99, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data for missing node, got %v", data)
	}
}

func TestLocalDiskBackend_ListEpochs(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	// No epochs initially.
	epochs, err := backend.ListEpochs()
	if err != nil {
		t.Fatalf("list epochs: %v", err)
	}
	if len(epochs) != 0 {
		t.Fatalf("expected 0 epochs, got %d", len(epochs))
	}

	// Save at epochs 3, 1, 5 (out of order).
	for _, e := range []uint64{3, 1, 5} {
		if err := backend.Save(e, "node", []byte("data")); err != nil {
			t.Fatalf("save epoch %d: %v", e, err)
		}
	}

	epochs, err = backend.ListEpochs()
	if err != nil {
		t.Fatalf("list epochs: %v", err)
	}
	if len(epochs) != 3 {
		t.Fatalf("expected 3 epochs, got %d", len(epochs))
	}
	// Should be sorted ascending.
	if epochs[0] != 1 || epochs[1] != 3 || epochs[2] != 5 {
		t.Fatalf("expected [1,3,5], got %v", epochs)
	}
}

func TestLocalDiskBackend_Delete(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	if err := backend.Save(1, "node", []byte("data")); err != nil {
		t.Fatal(err)
	}

	// Verify file exists.
	stateFile := filepath.Join(dir, "1", "node.state")
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("state file should exist: %v", err)
	}

	if err := backend.Delete(1); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify directory is gone.
	epochDir := filepath.Join(dir, "1")
	if _, err := os.Stat(epochDir); !os.IsNotExist(err) {
		t.Fatal("epoch directory should not exist after delete")
	}

	// Delete non-existent epoch is a no-op.
	if err := backend.Delete(99); err != nil {
		t.Fatalf("delete nonexistent: %v", err)
	}
}

func TestLocalDiskBackend_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	// Write and overwrite to verify atomic rename behavior.
	if err := backend.Save(1, "node", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Save(1, "node", []byte("second")); err != nil {
		t.Fatal(err)
	}

	data, err := backend.Load(1, "node")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "second" {
		t.Fatalf("expected 'second', got '%s'", data)
	}

	// Verify no temp files remain.
	entries, _ := os.ReadDir(filepath.Join(dir, "1"))
	for _, e := range entries {
		if e.Name() != "node.state" {
			t.Fatalf("unexpected file: %s", e.Name())
		}
	}
}

func TestLocalDiskBackend_IgnoresNonEpochDirs(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	// Create a non-epoch directory.
	os.MkdirAll(filepath.Join(dir, "not-a-number"), 0o755)
	// Create a file (not a directory).
	os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("hi"), 0o644)

	if err := backend.Save(1, "node", []byte("data")); err != nil {
		t.Fatal(err)
	}

	epochs, err := backend.ListEpochs()
	if err != nil {
		t.Fatal(err)
	}
	if len(epochs) != 1 || epochs[0] != 1 {
		t.Fatalf("expected [1], got %v", epochs)
	}
}
