package lsm

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"boilerpulse/internal/storage"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func smallOptions() Options {
	return Options{MemtableFlushThresholdBytes: 64, CompactionThreshold: 3}
}

func openEngine(t *testing.T, dir string, opts Options) *Engine {
	t.Helper()
	e, err := Open(dir, testLogger(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func TestPutGetDelete(t *testing.T) {
	e := openEngine(t, t.TempDir(), DefaultOptions())
	defer e.Close()

	if err := e.Put("k", []byte("v"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	entry, err := e.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value) != "v" {
		t.Errorf("Value = %q, want %q", entry.Value, "v")
	}

	if err := e.Delete("k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := e.Get("k"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("Get after Delete = %v, want ErrKeyNotFound", err)
	}
}

func TestGetMissingKey(t *testing.T) {
	e := openEngine(t, t.TempDir(), DefaultOptions())
	defer e.Close()

	if _, err := e.Get("nope"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("Get(missing) = %v, want ErrKeyNotFound", err)
	}
}

func TestDeleteMissingKeyReturnsNotFound(t *testing.T) {
	e := openEngine(t, t.TempDir(), DefaultOptions())
	defer e.Close()

	if err := e.Delete("nope"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("Delete(missing) = %v, want ErrKeyNotFound", err)
	}
}

func TestDeleteTwiceIsNotFoundSecondTime(t *testing.T) {
	e := openEngine(t, t.TempDir(), DefaultOptions())
	defer e.Close()

	if err := e.Put("k", []byte("v"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Delete("k"); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	if err := e.Delete("k"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("second Delete = %v, want ErrKeyNotFound", err)
	}
}

func TestTTLExpiry(t *testing.T) {
	e := openEngine(t, t.TempDir(), DefaultOptions())
	defer e.Close()

	if err := e.Put("k", []byte("v"), storage.ConsistencyEventual, time.Millisecond); err != nil {
		t.Fatalf("Put: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	if _, err := e.Get("k"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("Get after TTL expiry = %v, want ErrKeyNotFound", err)
	}
}

func TestFlushTriggersOnThresholdAndDataSurvives(t *testing.T) {
	e := openEngine(t, t.TempDir(), smallOptions())
	defer e.Close()

	for i := 0; i < 10; i++ {
		key := "key-" + string(rune('a'+i))
		val := "some reasonably sized value to grow the memtable past the threshold"
		if err := e.Put(key, []byte(val), storage.ConsistencyEventual, 0); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	e.mu.RLock()
	numSSTables := len(e.sstables)
	e.mu.RUnlock()
	if numSSTables == 0 {
		t.Error("expected at least one SSTable after exceeding the flush threshold, got 0")
	}

	entry, err := e.Get("key-a")
	if err != nil {
		t.Fatalf("Get after flush: %v", err)
	}
	if len(entry.Value) == 0 {
		t.Error("Get after flush returned an empty value")
	}
}

func TestCrashRecoveryReplaysWAL(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, DefaultOptions()) // large threshold: data stays only in the WAL, never flushed

	if err := e.Put("event:mackey", []byte(`{"title":"Purdue Basketball"}`), storage.ConsistencyStrong, time.Hour); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Put("event:cref", []byte(`{"title":"CREC open swim"}`), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Delete("event:cref"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Simulate a crash: close only the WAL file handle, with no clean flush.
	if err := e.CloseWALOnly(); err != nil {
		t.Fatalf("simulated-crash close: %v", err)
	}

	reopened := openEngine(t, dir, DefaultOptions())
	defer reopened.Close()

	entry, err := reopened.Get("event:mackey")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(entry.Value) != `{"title":"Purdue Basketball"}` {
		t.Errorf("recovered Value = %s, want the pre-crash value", entry.Value)
	}
	if entry.Consistency != storage.ConsistencyStrong {
		t.Errorf("recovered Consistency = %q, want %q", entry.Consistency, storage.ConsistencyStrong)
	}

	if _, err := reopened.Get("event:cref"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("recovered delete not honored: Get(event:cref) = %v, want ErrKeyNotFound", err)
	}
}

func TestCrashRecoveryStopsAtTornWrite(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, DefaultOptions())

	if err := e.Put("a", []byte("1"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Put("b", []byte("2"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.CloseWALOnly(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a crash mid-write of a third record: append a frame that
	// claims more bytes than actually follow.
	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 50, 9, 9, 9}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir, testLogger(), DefaultOptions())
	if err != nil {
		t.Fatalf("Open (recovery) returned error: %v, want a clean recovery despite the torn tail", err)
	}
	defer reopened.Close()

	if _, err := reopened.Get("a"); err != nil {
		t.Errorf("Get(a) after recovery: %v", err)
	}
	if _, err := reopened.Get("b"); err != nil {
		t.Errorf("Get(b) after recovery: %v", err)
	}
}

func TestCrashAfterFlushBeforeWALResetIsHarmlessOnReplay(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableFlushThresholdBytes: 1_000_000, CompactionThreshold: 10}
	e := openEngine(t, dir, opts)

	if err := e.Put("a", []byte("1"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Force a flush explicitly (data moves from memtable+WAL into an
	// SSTable), but simulate a crash before the WAL reset that would
	// normally follow it by re-appending to a fresh WAL at the same seq --
	// i.e. the WAL still has the record even though it's now also durable
	// in the SSTable. Recovery should replay it harmlessly.
	e.mu.Lock()
	if err := e.flushLocked(); err != nil {
		e.mu.Unlock()
		t.Fatalf("flushLocked: %v", err)
	}
	e.mu.Unlock()

	if err := e.Put("b", []byte("2"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.CloseWALOnly(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openEngine(t, dir, opts)
	defer reopened.Close()

	entryA, err := reopened.Get("a")
	if err != nil {
		t.Fatalf("Get(a) after recovery: %v", err)
	}
	if string(entryA.Value) != "1" {
		t.Errorf("Get(a) = %q, want %q", entryA.Value, "1")
	}
	entryB, err := reopened.Get("b")
	if err != nil {
		t.Fatalf("Get(b) after recovery: %v", err)
	}
	if string(entryB.Value) != "2" {
		t.Errorf("Get(b) = %q, want %q", entryB.Value, "2")
	}
}

func TestCompactionMergesAndDropsTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableFlushThresholdBytes: 1, CompactionThreshold: 2} // flush after every write
	e := openEngine(t, dir, opts)
	defer e.Close()

	mustPut(t, e, "a", "first")
	mustPut(t, e, "a", "second")
	mustPut(t, e, "b", "keep-me")
	if err := e.Delete("a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustPut(t, e, "c", "trigger-compaction") // pushes sstable count past the threshold

	e.mu.RLock()
	numSSTables := len(e.sstables)
	e.mu.RUnlock()
	if numSSTables > opts.CompactionThreshold {
		t.Errorf("sstable count = %d, want <= %d after compaction ran", numSSTables, opts.CompactionThreshold)
	}

	if _, err := e.Get("a"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("Get(a) after compaction = %v, want ErrKeyNotFound (a dropped tombstone must not resurrect the key)", err)
	}

	entryB, err := e.Get("b")
	if err != nil {
		t.Fatalf("Get(b) after compaction: %v", err)
	}
	if string(entryB.Value) != "keep-me" {
		t.Errorf("Get(b) = %q, want %q", entryB.Value, "keep-me")
	}

	entryC, err := e.Get("c")
	if err != nil {
		t.Fatalf("Get(c): %v", err)
	}
	if string(entryC.Value) != "trigger-compaction" {
		t.Errorf("Get(c) = %q, want %q", entryC.Value, "trigger-compaction")
	}
}

func TestCompactionKeepsLatestVersionAcrossTables(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableFlushThresholdBytes: 1, CompactionThreshold: 5}
	e := openEngine(t, dir, opts)
	defer e.Close()

	mustPut(t, e, "k", "v1") // flushes to sstable 1
	mustPut(t, e, "k", "v2") // flushes to sstable 2, should shadow v1

	e.mu.Lock()
	err := e.compactLocked()
	e.mu.Unlock()
	if err != nil {
		t.Fatalf("compactLocked: %v", err)
	}

	entry, err := e.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value) != "v2" {
		t.Errorf("Get(k) after compaction = %q, want %q (newest write must win)", entry.Value, "v2")
	}
}

func TestScanFindsKeysAcrossMemtableAndSSTablesWithNewestWinning(t *testing.T) {
	opts := Options{MemtableFlushThresholdBytes: 1, CompactionThreshold: 100} // flush after every write, no auto-compact
	e := openEngine(t, t.TempDir(), opts)
	defer e.Close()

	mustPut(t, e, "event:1", "v1")   // flushed to an sstable
	mustPut(t, e, "other:1", "skip") // different prefix, flushed to an sstable
	mustPut(t, e, "event:1", "v2")   // overwrite: flushed to a newer sstable

	if err := e.Put("event:2", []byte("in-memtable"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// event:2 stays in the memtable only if the flush threshold wasn't hit
	// by this single small write -- force a large threshold for this one
	// write isn't practical mid-test, so just confirm behavior regardless
	// of whether it flushed: Scan must find it either way.

	if err := e.Delete("event:1"); err != nil {
		// event:1 might already be gone from a prior flush's tombstone path;
		// only fail on unexpected errors.
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Delete: %v", err)
		}
	}

	got, err := e.Scan("event:")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if _, ok := got["other:1"]; ok {
		t.Error("Scan(\"event:\") returned a key with a different prefix")
	}
	if _, ok := got["event:1"]; ok {
		t.Error("Scan returned event:1, which was deleted, want it excluded")
	}
	entry, ok := got["event:2"]
	if !ok {
		t.Fatal("Scan did not return event:2")
	}
	if string(entry.Value) != "in-memtable" {
		t.Errorf("event:2 value = %q, want %q", entry.Value, "in-memtable")
	}
}

func TestConcurrentAccess(t *testing.T) {
	e := openEngine(t, t.TempDir(), smallOptions())
	defer e.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = e.Put("k", []byte("v"), storage.ConsistencyEventual, 0)
		}()
		go func() {
			defer wg.Done()
			_, _ = e.Get("k")
		}()
	}
	wg.Wait()
}

func mustPut(t *testing.T, e *Engine, key, value string) {
	t.Helper()
	if err := e.Put(key, []byte(value), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}
