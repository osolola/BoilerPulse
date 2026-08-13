package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/sstable"
)

func (e *Engine) maybeFlushLocked() error {
	if e.memtableSize < e.opts.MemtableFlushThresholdBytes {
		return nil
	}
	return e.flushLocked()
}

// flushLocked freezes the current memtable into a new SSTable and starts a
// fresh, empty memtable. The on-disk sequence is: write to a temp file,
// fsync, atomically rename into place, fsync the containing directory, and
// only then reset the WAL.
//
// That ordering is what makes a crash mid-flush safe: if we crash before
// the rename, the temp file is orphaned (cleaned up on next Open) and the
// WAL still holds every record needed to rebuild the same memtable. If we
// crash after the rename but before the WAL reset, replay simply re-applies
// records that are already reflected in the new SSTable — harmless,
// because applying the same Set/Delete twice is idempotent.
func (e *Engine) flushLocked() error {
	if len(e.memtable) == 0 {
		return nil
	}

	keys := make([]string, 0, len(e.memtable))
	for k := range e.memtable {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]sstable.Entry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, toSSTableEntry(k, e.memtable[k]))
	}

	table, path, err := e.writeAndOpenSSTable(entries)
	if err != nil {
		return fmt.Errorf("flushing memtable: %w", err)
	}

	if err := e.wal.Reset(); err != nil {
		return fmt.Errorf("resetting WAL after flush: %w", err)
	}

	e.sstables = append(e.sstables, sstableEntry{path: path, table: table})
	e.memtable = make(map[string]storage.Entry)
	e.memtableSize = 0

	e.logger.Info("flushed memtable to sstable", "file", filepath.Base(path), "entries", len(entries))

	if len(e.sstables) > e.opts.CompactionThreshold {
		if err := e.compactLocked(); err != nil {
			return fmt.Errorf("compacting: %w", err)
		}
	}

	return nil
}

// writeAndOpenSSTable implements the write-temp / fsync / rename / fsync-dir
// protocol shared by flush and compaction, returning the newly opened table
// and its final path.
func (e *Engine) writeAndOpenSSTable(entries []sstable.Entry) (*sstable.Table, string, error) {
	e.sstableSeq++
	finalName := sstableFileName(e.sstableSeq)
	sstablesDir := filepath.Join(e.dataDir, "sstables")
	tmpPath := filepath.Join(sstablesDir, "tmp-"+finalName)
	finalPath := filepath.Join(sstablesDir, finalName)

	if err := sstable.WriteSorted(tmpPath, entries); err != nil {
		return nil, "", fmt.Errorf("writing sstable: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, "", fmt.Errorf("finalizing sstable: %w", err)
	}
	if err := syncDir(sstablesDir); err != nil {
		return nil, "", fmt.Errorf("syncing sstables dir: %w", err)
	}

	table, err := sstable.Open(finalPath)
	if err != nil {
		return nil, "", fmt.Errorf("reopening flushed sstable: %w", err)
	}
	return table, finalPath, nil
}

func sstableFileName(seq int) string {
	return fmt.Sprintf("%06d.sst", seq)
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func toSSTableEntry(key string, e storage.Entry) sstable.Entry {
	se := sstable.Entry{
		Key:         key,
		Value:       e.Value,
		Consistency: string(e.Consistency),
		Tombstone:   e.Tombstone,
		Version:     e.Version,
	}
	if !e.ExpiresAt.IsZero() {
		se.ExpiresAtUnixNano = e.ExpiresAt.UnixNano()
	}
	return se
}
