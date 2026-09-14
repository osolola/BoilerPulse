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

// walWriter is the subset of *wal.Writer's API Engine depends on --
// exists so tests can substitute a fake that counts AppendBatch calls,
// proving concurrent writes actually get batched into fewer fsyncs rather
// than just asserting on end-state correctness.
type walWriter interface {
	AppendBatch(records []wal.Record) error
	Reset() error
	Close() error
}

var _ walWriter = (*wal.Writer)(nil)

// engineOp is one Put or Delete queued for opLoop's group commit: the
// already-built WAL record (seq assigned, so its position in the log is
// fixed the moment it's enqueued), the memtable mutation to apply once that
// record is durable, and where to send the result.
type engineOp struct {
	rec      wal.Record
	apply    func() // mutates the memtable; called only after this op's batch is durably fsynced, and only while holding e.mu
	resultCh chan error
}

// Engine is a WAL + memtable + SSTable storage.Engine implementation.
//
// Writes are group-committed: Put/Delete assign a sequence number and
// enqueue their WAL record and memtable mutation to opCh while briefly
// holding e.mu (just long enough to make enqueue order match seq order),
// then release the lock and wait for the result. A single background
// goroutine (opLoop) drains whatever has accumulated on opCh, writes every
// queued record with ONE trailing fsync (wal.Writer.AppendBatch), and only
// then applies every mutation to the memtable in the same order, under
// e.mu. This is what lets concurrent writers actually share a single
// fsync instead of each paying for their own, serialized one at a time --
// see docs/storage-engine.md and docs/benchmarking.md for the write-
// throughput ceiling this closes.
//
// Flush and compaction still run synchronously, inline with whichever
// opLoop batch crosses the flush threshold, while holding e.mu. That
// trades a latency spike on that batch for a much simpler correctness
// story than a background-flush design (which needs an immutable, still-
// readable "frozen" memtable while a new one accepts writes) would
// require. Deliberate simplification for this milestone — see
// docs/storage-engine.md.
type Engine struct {
	mu sync.RWMutex

	dataDir string
	logger  *slog.Logger
	opts    Options

	wal          walWriter
	memtable     map[string]storage.Entry
	memtableSize int

	sstables   []sstableEntry // ascending age: sstables[0] is oldest
	sstableSeq int

	nextSeq uint64

	opCh     chan engineOp
	stopCh   chan struct{}
	opDoneCh chan struct{}
	stopOnce sync.Once
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

	e := &Engine{
		dataDir:      dataDir,
		logger:       logger,
		opts:         opts,
		wal:          w,
		memtable:     memtable,
		memtableSize: memtableSize,
		sstables:     sstables,
		sstableSeq:   seq,
		nextSeq:      maxVersion + 1,
		// Buffered generously: this is what lets a burst of concurrent
		// Put/Delete calls actually pile up for opLoop to batch, rather
		// than each blocking on the enqueue itself (which would just
		// reintroduce the same one-at-a-time serialization this exists to
		// avoid). A full buffer still just makes a caller wait to enqueue
		// -- never incorrect, only less effective at batching.
		opCh:     make(chan engineOp, 1024),
		stopCh:   make(chan struct{}),
		opDoneCh: make(chan struct{}),
	}
	go e.opLoop()
	return e, nil
}

func (e *Engine) Get(key string) (storage.Entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lookupLocked(key)
}

func (e *Engine) Put(key string, value []byte, consistency storage.Consistency, ttl time.Duration) error {
	now := time.Now()
	var expiresAt time.Time
	var expiresAtNano int64
	if ttl > 0 {
		expiresAt = now.Add(ttl)
		expiresAtNano = expiresAt.UnixNano()
	}
	stored := make([]byte, len(value))
	copy(stored, value)

	e.mu.Lock()
	seq := e.nextSeq
	e.nextSeq++
	entry := storage.Entry{Value: stored, Consistency: consistency, ExpiresAt: expiresAt, Version: seq}
	op := engineOp{
		rec: wal.Record{
			Seq: seq, Op: wal.OpSet, Timestamp: now.UnixNano(), ExpiresAtUnixNano: expiresAtNano,
			Key: key, Consistency: string(consistency), Value: stored,
		},
		apply:    func() { e.applyToMemtable(key, entry) },
		resultCh: make(chan error, 1),
	}
	e.opCh <- op // enqueue while still holding e.mu, so enqueue order == seq order
	e.mu.Unlock()

	if err := <-op.resultCh; err != nil {
		return fmt.Errorf("appending to WAL: %w", err)
	}
	return nil
}

func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	if _, err := e.lookupLocked(key); err != nil {
		e.mu.Unlock()
		return err
	}

	seq := e.nextSeq
	e.nextSeq++
	op := engineOp{
		rec:      wal.Record{Seq: seq, Op: wal.OpDelete, Timestamp: time.Now().UnixNano(), Key: key},
		apply:    func() { e.applyToMemtable(key, storage.Entry{Tombstone: true, Version: seq}) },
		resultCh: make(chan error, 1),
	}
	e.opCh <- op
	e.mu.Unlock()

	if err := <-op.resultCh; err != nil {
		return fmt.Errorf("appending to WAL: %w", err)
	}
	return nil
}

// opLoop is the sole writer to the WAL file and the sole applier of
// memtable mutations from Put/Delete (HandleAppendEntries-style callers,
// i.e. every write goes through here) — started by Open, stopped by
// Close/CloseWALOnly. Processing one batch at a time on a single goroutine
// is what guarantees records land in the WAL, and mutations land in the
// memtable, in exactly seq order even though the ops within one batch were
// enqueued by different concurrent callers.
func (e *Engine) opLoop() {
	defer close(e.opDoneCh)
	for {
		select {
		case <-e.stopCh:
			return
		case first := <-e.opCh:
			batch := []engineOp{first}
		drain:
			for {
				select {
				case op := <-e.opCh:
					batch = append(batch, op)
				default:
					break drain
				}
			}
			e.processBatch(batch)
		}
	}
}

// processBatch writes every op's WAL record with a single trailing fsync,
// then (only if that succeeded) applies every op's memtable mutation in
// order and checks once whether the batch pushed the memtable past the
// flush threshold.
func (e *Engine) processBatch(batch []engineOp) {
	records := make([]wal.Record, len(batch))
	for i, op := range batch {
		records[i] = op.rec
	}

	if err := e.wal.AppendBatch(records); err != nil {
		for _, op := range batch {
			op.resultCh <- err
		}
		return
	}

	e.mu.Lock()
	for _, op := range batch {
		op.apply()
	}
	flushErr := e.maybeFlushLocked()
	e.mu.Unlock()

	for _, op := range batch {
		op.resultCh <- flushErr
	}
}

// Close performs an orderly shutdown: stops opLoop, closes the WAL, and
// closes all open SSTable file handles. It does not flush the memtable —
// durability already comes from per-batch WAL fsyncs, not from a
// flush-on-close.
func (e *Engine) Close() error {
	e.stop()

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
	e.stop()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.Close()
}

// stop halts opLoop and waits for it to exit, so neither Close variant
// closes the WAL file out from under a batch that's still being written.
// Safe to call more than once.
func (e *Engine) stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
		<-e.opDoneCh
	})
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

// Stats is a point-in-time view of engine internals, for observability
// (internal/metrics scrapes this). It is deliberately a snapshot under the
// read lock rather than live counters: these values are cheap to read and
// only meaningful together.
type Stats struct {
	MemtableBytes   int
	MemtableEntries int
	SSTables        int
}

// Stats returns a snapshot of engine internals.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{
		MemtableBytes:   e.memtableSize,
		MemtableEntries: len(e.memtable),
		SSTables:        len(e.sstables),
	}
}
