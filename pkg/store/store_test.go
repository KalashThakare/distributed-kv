package store

import (
	"fmt"
	"path/filepath"
	"testing"
)

// tempDir creates a temporary directory for test files.
// t.TempDir() automatically deletes it after the test.
func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

// openTestStore creates a Store with WAL and bbolt in a temp directory.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := tempDir(t)
	s, err := Open(Config{
		WALPath:  filepath.Join(dir, "test.wal"),
		BoltPath: filepath.Join(dir, "test.db"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func TestStore_PutGet(t *testing.T) {
	s := openTestStore(t)
	if err := s.Put("user:rahul", "Rahul Sharma"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get("user:rahul")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "Rahul Sharma" {
		t.Errorf("want %q, got %q", "Rahul Sharma", got)
	}
}
func TestStore_GetMissing(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("want ErrKeyNotFound, got %v", err)
	}
}
func TestStore_Delete(t *testing.T) {
	s := openTestStore(t)
	_ = s.Put("temp:key", "value")
	if err := s.Delete("temp:key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get("temp:key")
	if err != ErrKeyNotFound {
		t.Errorf("want ErrKeyNotFound after delete, got %v", err)
	}
}
func TestStore_DeleteIdempotent(t *testing.T) {
	s := openTestStore(t)
	// Deleting a non-existent key must not error
	if err := s.Delete("never:existed"); err != nil {
		t.Errorf("Delete non-existent: want nil, got %v", err)
	}
}
func TestStore_Size(t *testing.T) {
	s := openTestStore(t)
	for i := 0; i < 10; i++ {
		_ = s.Put(fmt.Sprintf("key:%d", i), "value")
	}
	if got := s.Size(); got != 10 {
		t.Errorf("want 10, got %d", got)
	}
	_ = s.Delete("key:0")
	if got := s.Size(); got != 9 {
		t.Errorf("after delete: want 9, got %d", got)
	}
}

func TestStore_WALRecovery(t *testing.T) {
	dir := tempDir(t)
	walPath := filepath.Join(dir, "recovery.wal")
	boltPath := filepath.Join(dir, "recovery.db")
	// === Phase 1: write data and simulate a crash ===
	s1, err := Open(Config{WALPath: walPath, BoltPath: boltPath})
	if err != nil {
		t.Fatalf("Open phase 1: %v", err)
	}
	keys := []string{"user:1", "user:2", "user:3", "cart:abc"}
	for _, k := range keys {
		if err := s1.Put(k, "value:"+k); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}
	// Simulate crash: close bbolt but do NOT flush to bbolt.
	// The WAL has the entries; bbolt does NOT.
	// Calling Close() would flush — so we forcibly close the underlying db.
	_ = s1.db.Close()  // simulate crash (bypass flush)
	_ = s1.wal.Close() // ← add this: release the file handle on Windows
	s1.db = nil        // prevent s2.Close() from double-closing
	s1.wal = nil
	// bypass Clean Close to simulate crash
	// === Phase 2: reopen and verify WAL replay restored all data ===
	s2, err := Open(Config{WALPath: walPath, BoltPath: boltPath})
	if err != nil {
		t.Fatalf("Open phase 2: %v", err)
	}
	defer s2.Close()
	for _, k := range keys {
		got, err := s2.Get(k)
		if err != nil {
			t.Errorf("after recovery, Get(%q): %v", k, err)
			continue
		}
		want := "value:" + k
		if got != want {
			t.Errorf("after recovery, Get(%q) = %q, want %q", k, got, want)
		}
	}
	t.Logf("WAL recovery successful: %d keys restored", len(keys))
}
func TestStore_WALReplayDelete(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		WALPath:  filepath.Join(dir, "del.wal"),
		BoltPath: filepath.Join(dir, "del.db"),
	}
	s1, _ := Open(cfg)
	_ = s1.Put("to:delete", "exists")
	_ = s1.Delete("to:delete")
	_ = s1.db.Close()
	_ = s1.wal.Close() // ← same fix here
	s1.db = nil
	s1.wal = nil
	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	_, err = s2.Get("to:delete")
	if err != ErrKeyNotFound {
		t.Errorf("deleted key survived recovery: want ErrKeyNotFound, got %v", err)
	}
}

// BenchmarkStore_Put measures Put throughput with WAL enabled.
// Run: go test ./pkg/store/... -bench=BenchmarkStore_Put -benchtime=10s
func BenchmarkStore_Put(b *testing.B) {
	dir := b.TempDir()
	s, err := Open(Config{
		WALPath:  filepath.Join(dir, "bench.wal"),
		BoltPath: filepath.Join(dir, "bench.db"),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := s.Put(key, "benchmark-value"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkStore_Get measures Get throughput (read-only hot path).
// Target: > 5,000,000 Get ops/sec (memory only, no disk).
func BenchmarkStore_Get(b *testing.B) {
	s, err := Open(Config{}) // memory only for this benchmark
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	// Pre-populate
	for i := 0; i < 10000; i++ {
		_ = s.Put(fmt.Sprintf("key:%d", i), "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get(fmt.Sprintf("key:%d", i%10000))
	}
	b.ReportAllocs()
}

// BenchmarkStore_PutMemOnly measures Put with no WAL (pure memory speed).
// Useful to understand WAL overhead.
func BenchmarkStore_PutMemOnly(b *testing.B) {
	s, _ := Open(Config{}) // no WAL, no bbolt
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Put(fmt.Sprintf("key:%d", i), "value")
	}
}
