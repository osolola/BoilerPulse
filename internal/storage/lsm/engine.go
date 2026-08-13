// Package lsm implements a durable storage.Engine: writes are fsynced to a
// write-ahead log before being applied to an in-memory memtable; the
// memtable flushes to an immutable SSTable once it grows past a configured
// size; SSTables merge via compaction once they accumulate past a
// configured count. See docs/storage-engine.md for the on-disk format and
// crash-safety invariants.
package lsm

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/sstable"
	"boilerpulse/internal/storage/wal"
)

var _ storage.Engine = (*Engine)(nil)

// sstableEntry pairs an open Table with the file it was opened from, so
// compaction can remove the right files without reconstructing paths from
// naming conventions.
type sstableEntry struct {
	path  string
	table *sstable.Table
}

// Engine is a WAL + memtable + SSTable storage.Engine implementation.
//
// Flush and compaction both run synchronously, inline with the write that
// triggers them, while holding the engine's write lock. That trades a
// latency spike on the triggering write for a much simpler correctness
// story than a background-flush design (which needs an immutable, still-
// readable "frozen" memtable while a new one accepts writes) would require.
// This is a deliberate simplification for this milestone — see
// docs/storage-engine.md.
type Engine struct {
	mu sync.RWMutex

	dataDir string
	logger  *slog.Logger
	opts    Options

	wal          *wal.Writer
	memtable     map[string]storage.Entry
	memtableSize int

	sstables   []sstableEntry // ascending age: sstables[0] is oldest
	sstableSeq int

	nextSeq uint64
}

// Open recovers engine state from dataDir (existing SSTables, then WAL
// replay on top of them) and returns a ready-to-use Engine. A brand-new,
// empty dataDir is fine — it starts with no data.
func Open(dataDir string, logger *slog.Logger, opts Options) (*Engine, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "sstables"), 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	if err := cleanupOrphanedTempFiles(dataDir); err != nil {
		return nil, fmt.Errorf("cleaning up orphaned temp files: %w", err)
	}

	sstables, seq, maxVersion, err := loadSSTables(dataDir)
	if err != nil {
		return nil, err
	}

	walPath := filepath.Join(dataDir, "wal.log")
	records, err := wal.ReadAll(walPath)
	if err != nil {
		return nil, fmt.Errorf("reading WAL: %w", err)
	}

	memtable := make(map[string]storage.Entry)
	var memtableSize int
	for _, rec := range records {
		if rec.Seq > maxVersion {
			maxVersion = rec.Seq
		}
		applyRecordToMemtable(memtable, &memtableSize, rec)
	}

	w, err := wal.OpenWriter(walPath)
	if err != nil {
		return nil, fmt.Errorf("opening WAL: %w", err)
	}

	logger.Info("storage engine recovered",
		"sstables", len(sstables),
		"wal_records_replayed", len(records),
		"next_seq", maxVersion+1)

	return &Engine{
		dataDir:      dataDir,
		logger:       logger,
		opts:         opts,
		wal:          w,
		memtable:     memtable,
		memtableSize: memtableSize,
		sstables:     sstables,
		sstableSeq:   seq,
		nextSeq:      maxVersion + 1,
	}, nil
}

func (e *Engine) Get(key string) (storage.Entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lookupLocked(key)
}

func (e *Engine) Put(key string, value []byte, consistency storage.Consistency, ttl time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := e.nextSeq
	e.nextSeq++

	now := time.Now()
	var expiresAt time.Time
	var expiresAtNano int64
	if ttl > 0 {
		expiresAt = now.Add(ttl)
		expiresAtNano = expiresAt.UnixNano()
	}

	stored := make([]byte, len(value))
	copy(stored, value)

	rec := wal.Record{
		Seq:               seq,
		Op:                wal.OpSet,
		Timestamp:         now.UnixNano(),
		ExpiresAtUnixNano: expiresAtNano,
		Key:               key,
		Consistency:       string(consistency),
		Value:             stored,
	}
	if err := e.wal.Append(rec); err != nil {
		return fmt.Errorf("appending to WAL: %w", err)
	}

	e.applyToMemtable(key, storage.Entry{
		Value:       stored,
		Consistency: consistency,
		ExpiresAt:   expiresAt,
		Version:     seq,
	})

	return e.maybeFlushLocked()
}

func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := e.lookupLocked(key); err != nil {
		return err
	}

	seq := e.nextSeq
	e.nextSeq++

	rec := wal.Record{Seq: seq, Op: wal.OpDelete, Timestamp: time.Now().UnixNano(), Key: key}
	if err := e.wal.Append(rec); err != nil {
		return fmt.Errorf("appending to WAL: %w", err)
	}

	e.applyToMemtable(key, storage.Entry{Tombstone: true, Version: seq})

	return e.maybeFlushLocked()
}

// Close performs an orderly shutdown: closes the WAL and all open SSTable
// file handles. It does not flush the memtable — durability already comes
// from per-write WAL fsyncs, not from a flush-on-close.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var firstErr error
	if err := e.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, st := range e.sstables {
		if err := st.table.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CloseWALOnly closes just the WAL file handle, leaving SSTables and the
// memtable untouched. It exists so tests (including ones in other packages)
// can simulate an unclean shutdown — a real crash wouldn't get a chance to
// flush or close SSTables cleanly either. Prefer Close for a real shutdown.
func (e *Engine) CloseWALOnly() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.Close()
}

func (e *Engine) lookupLocked(key string) (storage.Entry, error) {
	if entry, ok := e.memtable[key]; ok {
		return checkedEntry(entry)
	}

	for i := len(e.sstables) - 1; i >= 0; i-- {
		entry, found, err := e.sstables[i].table.Get(key)
		if err != nil {
			return storage.Entry{}, fmt.Errorf("reading sstable %s: %w", e.sstables[i].path, err)
		}
		if found {
			return checkedEntry(toStorageEntry(entry))
		}
	}

	return storage.Entry{}, storage.ErrKeyNotFound
}

// Scan merges every SSTable (oldest to newest) with the memtable (always
// newest) to find every live key with the given prefix, in the same
// precedence order Get uses. It's a full scan of every table, not an
// indexed prefix query — fine at this project's scale (see
// docs/storage-engine.md), and only used for GET /v1/events, not on any
// hot path.
func (e *Engine) Scan(prefix string) (map[string]storage.Entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	merged := make(map[string]storage.Entry)
	for _, st := range e.sstables {
		entries, err := st.table.Iterate()
		if err != nil {
			return nil, fmt.Errorf("iterating sstable %s: %w", st.path, err)
		}
		for _, se := range entries {
			if strings.HasPrefix(se.Key, prefix) {
				merged[se.Key] = toStorageEntry(se)
			}
		}
	}
	for k, v := range e.memtable {
		if strings.HasPrefix(k, prefix) {
			merged[k] = v
		}
	}

	now := time.Now()
	result := make(map[string]storage.Entry, len(merged))
	for k, v := range merged {
		if v.Tombstone || v.Expired(now) {
			continue
		}
		result[k] = v
	}
	return result, nil
}

func (e *Engine) applyToMemtable(key string, entry storage.Entry) {
	if old, ok := e.memtable[key]; ok {
		e.memtableSize -= entrySize(key, old)
	}
	e.memtable[key] = entry
	e.memtableSize += entrySize(key, entry)
}

func checkedEntry(e storage.Entry) (storage.Entry, error) {
	if e.Tombstone || e.Expired(time.Now()) {
		return storage.Entry{}, storage.ErrKeyNotFound
	}
	return e, nil
}

func toStorageEntry(e sstable.Entry) storage.Entry {
	se := storage.Entry{
		Value:       e.Value,
		Consistency: storage.Consistency(e.Consistency),
		Tombstone:   e.Tombstone,
		Version:     e.Version,
	}
	if e.ExpiresAtUnixNano != 0 {
		se.ExpiresAt = time.Unix(0, e.ExpiresAtUnixNano)
	}
	return se
}

// entrySize is a rough accounting estimate (key + value bytes plus a fixed
// per-entry overhead) used only to decide when to flush — it doesn't need
// to be exact.
func entrySize(key string, e storage.Entry) int {
	return len(key) + len(e.Value) + 32
}
