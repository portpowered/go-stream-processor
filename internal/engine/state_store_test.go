package engine

import (
	"testing"
)

func TestInMemoryStateStore_GetPutDelete(t *testing.T) {
	store := NewInMemoryStateStore()

	// Get on empty store returns nil.
	if v := store.Get("key1"); v != nil {
		t.Fatalf("expected nil, got %v", v)
	}

	// Put and Get.
	store.Put("key1", []byte("value1"))
	if v := store.Get("key1"); string(v) != "value1" {
		t.Fatalf("expected 'value1', got '%s'", v)
	}

	// Overwrite.
	store.Put("key1", []byte("value1-updated"))
	if v := store.Get("key1"); string(v) != "value1-updated" {
		t.Fatalf("expected 'value1-updated', got '%s'", v)
	}

	// Delete.
	store.Delete("key1")
	if v := store.Get("key1"); v != nil {
		t.Fatalf("expected nil after delete, got %v", v)
	}

	// Delete non-existent key is a no-op.
	store.Delete("nonexistent")
}

func TestInMemoryStateStore_GetReturnsCopy(t *testing.T) {
	store := NewInMemoryStateStore()
	store.Put("key", []byte("original"))

	v := store.Get("key")
	v[0] = 'X' // Mutate the returned copy.

	// Original should be unchanged.
	if v2 := store.Get("key"); string(v2) != "original" {
		t.Fatalf("mutation leaked: expected 'original', got '%s'", v2)
	}
}

func TestInMemoryStateStore_PutStoresCopy(t *testing.T) {
	store := NewInMemoryStateStore()
	data := []byte("original")
	store.Put("key", data)

	data[0] = 'X' // Mutate the input after Put.

	if v := store.Get("key"); string(v) != "original" {
		t.Fatalf("mutation leaked: expected 'original', got '%s'", v)
	}
}

func TestInMemoryStateStore_SnapshotRestore(t *testing.T) {
	store := NewInMemoryStateStore()
	store.Put("a", []byte("1"))
	store.Put("b", []byte("22"))
	store.Put("c", []byte("333"))

	snap := store.Snapshot()
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}

	// Restore into a new store.
	store2 := NewInMemoryStateStore()
	if err := store2.Restore(snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	for _, key := range []string{"a", "b", "c"} {
		v1 := store.Get(key)
		v2 := store2.Get(key)
		if string(v1) != string(v2) {
			t.Fatalf("key %q: expected '%s', got '%s'", key, v1, v2)
		}
	}
}

func TestInMemoryStateStore_SnapshotEmpty(t *testing.T) {
	store := NewInMemoryStateStore()
	snap := store.Snapshot()
	if snap != nil {
		t.Fatalf("expected nil snapshot for empty store, got %v", snap)
	}

	// Restore nil/empty is a no-op.
	store2 := NewInMemoryStateStore()
	if err := store2.Restore(nil); err != nil {
		t.Fatalf("restore nil failed: %v", err)
	}
	if err := store2.Restore([]byte{}); err != nil {
		t.Fatalf("restore empty failed: %v", err)
	}
}

func TestInMemoryStateStore_RestoreCorrupt(t *testing.T) {
	store := NewInMemoryStateStore()

	// Truncated key length.
	if err := store.Restore([]byte{0, 0}); err == nil {
		t.Fatal("expected error for truncated data")
	}

	// Key length extends past data.
	if err := store.Restore([]byte{0, 0, 0, 10, 'a'}); err == nil {
		t.Fatal("expected error for truncated key")
	}

	// Truncated value length.
	if err := store.Restore([]byte{0, 0, 0, 1, 'a', 0, 0}); err == nil {
		t.Fatal("expected error for truncated value length")
	}

	// Value length extends past data.
	if err := store.Restore([]byte{0, 0, 0, 1, 'a', 0, 0, 0, 10, 'b'}); err == nil {
		t.Fatal("expected error for truncated value")
	}
}

func TestInMemoryStateStore_RestoreOverwrites(t *testing.T) {
	store := NewInMemoryStateStore()
	store.Put("old", []byte("data"))

	// Create snapshot with different data.
	other := NewInMemoryStateStore()
	other.Put("new", []byte("fresh"))
	snap := other.Snapshot()

	if err := store.Restore(snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if v := store.Get("old"); v != nil {
		t.Fatalf("expected old key to be gone, got '%s'", v)
	}
	if v := store.Get("new"); string(v) != "fresh" {
		t.Fatalf("expected 'fresh', got '%s'", v)
	}
}
