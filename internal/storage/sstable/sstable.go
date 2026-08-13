// Package sstable implements the on-disk sorted-string-table format: a
// sorted data block, a full key index, and a fixed-size footer. Like
// internal/storage/wal, it's format-focused and doesn't depend on
// internal/storage — internal/storage/lsm translates between the two.
package sstable

import "errors"

// Entry is a single key's stored value as persisted in an SSTable.
type Entry struct {
	Key               string
	Value             []byte // empty when Tombstone is true
	Consistency       string
	ExpiresAtUnixNano int64 // 0 means no expiry
	Tombstone         bool
	Version           uint64
}

const magic uint32 = 0x42505354 // "BPST"

// footer layout: indexOffset(8) + indexLen(8) + entryCount(8) + magic(4)
const footerSize = 8 + 8 + 8 + 4

// ErrInvalidFile means a file failed its magic-number check on Open — it is
// not a valid, fully-written SSTable. Since SSTables are only ever exposed
// under their final filename after a complete write + atomic rename (see
// internal/storage/lsm), this indicates unexpected corruption rather than a
// normal crash scenario, so it's surfaced as an error rather than silently
// skipped.
var ErrInvalidFile = errors.New("sstable: invalid or corrupt file")
