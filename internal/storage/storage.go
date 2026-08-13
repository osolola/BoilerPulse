// Package storage defines the key-value storage engine interface used by the
// KV API. The only implementation today is MemStore (in-memory, non-durable).
// A WAL/memtable/SSTable-backed implementation arrives in Milestone 2 and will
// satisfy the same Engine interface, so callers above this package don't change.
package storage

import (
	"errors"
	"time"
)

// ErrKeyNotFound is returned by Get and Delete when the key does not exist,
// is a tombstone, or has expired.
var ErrKeyNotFound = errors.New("key not found")

// Consistency is the per-key consistency policy from the KV API (spec §13).
// It is stored today but not yet enforced — enforcement (e.g. bypassing stale
// cache reads for CRITICAL) arrives with the caching/workload layer.
type Consistency string

const (
	ConsistencyStrong   Consistency = "STRONG"
	ConsistencyEventual Consistency = "EVENTUAL"
	ConsistencyCritical Consistency = "CRITICAL"
)

// Entry is a stored value plus its metadata.
type Entry struct {
	Value       []byte
	Consistency Consistency
	ExpiresAt   time.Time // zero value means no expiry
	Tombstone   bool
	Version     uint64
}

// Expired reports whether the entry's TTL has passed as of now.
func (e Entry) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)
}

// Engine is the storage contract the KV API depends on. MemStore is the
// pass-1 implementation; internal/storage/lsm.Engine (Milestone 2) is the
// durable one.
type Engine interface {
	Get(key string) (Entry, error)
	Put(key string, value []byte, consistency Consistency, ttl time.Duration) error
	Delete(key string) error
	// Scan returns every non-tombstoned, non-expired entry whose key has
	// the given prefix. Added in Milestone 5 for GET /v1/events (events are
	// stored as ordinary KV entries under an "event:" prefix, not a
	// separate storage system) — a deliberately simple full-prefix-scan,
	// not an indexed query engine.
	Scan(prefix string) (map[string]Entry, error)
}
