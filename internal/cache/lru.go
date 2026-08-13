// Package cache implements a small application-level LRU cache (spec §25).
// It's deliberately generic (string key, []byte value) rather than
// KV-specific — internal/gateway is what decides what's cacheable
// (notably: never CRITICAL-consistency data).
package cache

import (
	"container/list"
	"sync"
)

// Stats are cumulative counters since the cache was created.
type Stats struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
}

type entry struct {
	key   string
	value []byte
}

// LRU is a fixed-capacity, least-recently-used cache. Safe for concurrent use.
type LRU struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List // front = most recently used

	hits, misses, evictions int64
}

// NewLRU returns an empty cache holding at most capacity entries. A
// capacity <= 0 means "never cache anything" (Get always misses, Set is a
// no-op) — useful for disabling caching without a separate on/off flag.
func NewLRU(capacity int) *LRU {
	return &LRU{capacity: capacity, items: make(map[string]*list.Element), order: list.New()}
}

// Get returns the cached value for key, if present, moving it to
// most-recently-used.
func (c *LRU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	c.order.MoveToFront(elem)
	c.hits++
	return elem.Value.(*entry).value, true
}

// Set stores value under key, evicting the least-recently-used entry if the
// cache is over capacity.
func (c *LRU) Set(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity <= 0 {
		return
	}

	if elem, ok := c.items[key]; ok {
		elem.Value.(*entry).value = value
		c.order.MoveToFront(elem)
		return
	}

	elem := c.order.PushFront(&entry{key: key, value: value})
	c.items[key] = elem

	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(*entry).key)
			c.evictions++
		}
	}
}

// Delete removes key from the cache, if present. Used to invalidate a
// cached read after the underlying value changes.
func (c *LRU) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.order.Remove(elem)
		delete(c.items, key)
	}
}

// Stats returns cumulative hit/miss/eviction counters.
func (c *LRU) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Hits: c.hits, Misses: c.misses, Evictions: c.evictions}
}

// HitRate returns hits / (hits + misses), or 0 if there have been no
// lookups yet.
func (c *LRU) HitRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total)
}
