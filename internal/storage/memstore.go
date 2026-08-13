package storage

import (
	"strings"
	"sync"
	"time"
)

// MemStore is an in-memory Engine implementation: a mutex-guarded map with
// tombstone deletes and optional TTL expiry. It is not durable — a restart
// loses all data. Persistence arrives in Milestone 2 (WAL + SSTables).
type MemStore struct {
	mu      sync.RWMutex
	entries map[string]Entry
	version uint64
	now     func() time.Time
}

// NewMemStore returns an empty, ready-to-use in-memory engine.
func NewMemStore() *MemStore {
	return &MemStore{
		entries: make(map[string]Entry),
		now:     time.Now,
	}
}

func (m *MemStore) Get(key string) (Entry, error) {
	m.mu.RLock()
	entry, ok := m.entries[key]
	m.mu.RUnlock()

	if !ok || entry.Tombstone || entry.Expired(m.now()) {
		return Entry{}, ErrKeyNotFound
	}
	return entry, nil
}

func (m *MemStore) Put(key string, value []byte, consistency Consistency, ttl time.Duration) error {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = m.now().Add(ttl)
	}

	stored := make([]byte, len(value))
	copy(stored, value)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.version++
	m.entries[key] = Entry{
		Value:       stored,
		Consistency: consistency,
		ExpiresAt:   expiresAt,
		Version:     m.version,
	}
	return nil
}

func (m *MemStore) Scan(prefix string) (map[string]Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := m.now()
	result := make(map[string]Entry)
	for k, v := range m.entries {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if v.Tombstone || v.Expired(now) {
			continue
		}
		result[k] = v
	}
	return result, nil
}

func (m *MemStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.entries[key]
	if !ok || existing.Tombstone || existing.Expired(m.now()) {
		return ErrKeyNotFound
	}

	m.version++
	m.entries[key] = Entry{Tombstone: true, Version: m.version}
	return nil
}
