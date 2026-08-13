package lsm

// Options configures flush and compaction thresholds. Byte-based sizing
// mirrors how real LSM engines decide when to flush; tests use small values
// to trigger flush/compaction deterministically without writing megabytes
// of data.
type Options struct {
	// MemtableFlushThresholdBytes: once the memtable's estimated size
	// crosses this, the next write triggers a flush to a new SSTable.
	MemtableFlushThresholdBytes int
	// CompactionThreshold: once the SSTable count exceeds this after a
	// flush, all current SSTables are merged into one.
	CompactionThreshold int
}

// DefaultOptions returns production-sized thresholds.
func DefaultOptions() Options {
	return Options{
		MemtableFlushThresholdBytes: 4 * 1024 * 1024, // 4MB
		CompactionThreshold:         4,
	}
}
