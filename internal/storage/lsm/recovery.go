package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/sstable"
	"boilerpulse/internal/storage/wal"
)

// loadSSTables opens every finalized SSTable in dataDir/sstables, sorted
// oldest to newest by filename. It also returns the highest sstable
// sequence number seen (so new flushes continue numbering without
// collisions) and the highest entry Version seen across every table (so the
// WAL record sequence counter can continue past it after a restart).
//
// There is no separate manifest file recording these counters — reading
// every SSTable's contents on each Open to reconstruct them is a
// deliberate, demo-scale simplification; a production engine would persist
// them directly. See docs/storage-engine.md.
func loadSSTables(dataDir string) ([]sstableEntry, int, uint64, error) {
	dir := filepath.Join(dataDir, "sstables")
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("reading sstables dir: %w", err)
	}

	var names []string
	for _, de := range dirEntries {
		if !de.IsDir() && strings.HasSuffix(de.Name(), ".sst") && !strings.HasPrefix(de.Name(), "tmp-") {
			names = append(names, de.Name())
		}
	}
	sort.Strings(names) // zero-padded numeric names sort correctly as strings

	var tables []sstableEntry
	var maxSeq int
	var maxVersion uint64
	for _, name := range names {
		path := filepath.Join(dir, name)
		table, err := sstable.Open(path)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("opening sstable %s: %w", name, err)
		}
		tables = append(tables, sstableEntry{path: path, table: table})

		var n int
		if _, err := fmt.Sscanf(name, "%06d.sst", &n); err == nil && n > maxSeq {
			maxSeq = n
		}

		entries, err := table.Iterate()
		if err != nil {
			return nil, 0, 0, fmt.Errorf("iterating sstable %s: %w", name, err)
		}
		for _, e := range entries {
			if e.Version > maxVersion {
				maxVersion = e.Version
			}
		}
	}

	return tables, maxSeq, maxVersion, nil
}

// cleanupOrphanedTempFiles removes any tmp-*.sst files left behind by a
// flush or compaction that crashed before its atomic rename completed. Such
// files are guaranteed incomplete/unreferenced: a table only becomes
// "real" once rename gives it its final name.
func cleanupOrphanedTempFiles(dataDir string) error {
	dir := filepath.Join(dataDir, "sstables")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "tmp-") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRecordToMemtable replays one WAL record into memtable, mirroring
// what Put/Delete would have done live. Applying records in file order and
// letting later writes simply overwrite the map entry for the same key is
// sufficient for correct last-write-wins semantics — no explicit sequence
// comparison is needed during replay.
func applyRecordToMemtable(memtable map[string]storage.Entry, sizeAcc *int, rec wal.Record) {
	var entry storage.Entry
	switch rec.Op {
	case wal.OpSet:
		entry = storage.Entry{
			Value:       rec.Value,
			Consistency: storage.Consistency(rec.Consistency),
			Version:     rec.Seq,
		}
		if rec.ExpiresAtUnixNano != 0 {
			entry.ExpiresAt = time.Unix(0, rec.ExpiresAtUnixNano)
		}
	case wal.OpDelete:
		entry = storage.Entry{Tombstone: true, Version: rec.Seq}
	default:
		return
	}

	if old, ok := memtable[rec.Key]; ok {
		*sizeAcc -= entrySize(rec.Key, old)
	}
	memtable[rec.Key] = entry
	*sizeAcc += entrySize(rec.Key, entry)
}
