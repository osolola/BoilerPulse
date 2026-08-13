package lsm

import (
	"fmt"
	"os"
	"sort"
	"time"

	"boilerpulse/internal/storage/sstable"
)

// compactLocked merges every current SSTable into a single new one. Because
// the merge spans ALL tables (no leveled/tiered partial merges yet), it's
// safe to drop tombstones and already-expired entries outright: there is no
// older table left underneath that a dropped tombstone would need to keep
// shadowing. Newer tables' entries win over older tables' for the same key.
//
// This full-table-merge strategy is simpler than a real leveled/tiered
// compaction scheme, at the cost of rewriting more data than strictly
// necessary as the dataset grows — an acceptable tradeoff at this project's
// scale, and called out in docs/storage-engine.md as a known simplification.
func (e *Engine) compactLocked() error {
	start := time.Now()
	now := time.Now()

	merged := make(map[string]sstable.Entry)
	for _, st := range e.sstables {
		entries, err := st.table.Iterate()
		if err != nil {
			return fmt.Errorf("iterating sstable %s: %w", st.path, err)
		}
		for _, entry := range entries {
			merged[entry.Key] = entry // later (newer) tables overwrite earlier ones
		}
	}

	keys := make([]string, 0, len(merged))
	for k, entry := range merged {
		if entry.Tombstone {
			continue
		}
		if entry.ExpiresAtUnixNano != 0 && now.After(time.Unix(0, entry.ExpiresAtUnixNano)) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]sstable.Entry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, merged[k])
	}

	newTable, newPath, err := e.writeAndOpenSSTable(entries)
	if err != nil {
		return fmt.Errorf("writing compacted sstable: %w", err)
	}

	old := e.sstables
	e.sstables = []sstableEntry{{path: newPath, table: newTable}}

	// Only remove the old files once the merged table is fully durable on
	// disk (writeAndOpenSSTable already fsynced it and its directory
	// entry). If we crash before this cleanup completes, the worst case is
	// orphaned old files that are never read again, since e.sstables no
	// longer references them.
	for _, st := range old {
		_ = st.table.Close()
		_ = os.Remove(st.path)
	}

	e.logger.Info("compacted sstables",
		"merged_count", len(old),
		"result_entries", len(entries),
		"duration_ms", time.Since(start).Milliseconds())

	return nil
}
