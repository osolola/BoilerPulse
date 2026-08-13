package workload

import (
	"sort"
	"sync"
	"time"
)

// KeyStat is one key's observed request count within the tracking window.
type KeyStat struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// HotKeyTracker counts requests per key over a sliding window (spec §12:
// requests/key, recent growth) and reports which keys cross a threshold.
// Like RequestMonitor, it buckets by millisecond, not whole seconds — see
// RequestMonitor's doc comment for why that matters.
type HotKeyTracker struct {
	mu        sync.Mutex
	window    time.Duration
	threshold int64
	keyHits   map[string]map[int64]int64 // key -> unix milli -> count
}

// NewHotKeyTracker tracks per-key request counts over window, treating a
// key as "hot" once its count within the window reaches threshold.
func NewHotKeyTracker(window time.Duration, threshold int64) *HotKeyTracker {
	return &HotKeyTracker{window: window, threshold: threshold, keyHits: make(map[string]map[int64]int64)}
}

// Record counts one request for key at the current time.
func (t *HotKeyTracker) Record(key string) {
	if key == "" {
		return
	}
	now := time.Now().UnixMilli()

	t.mu.Lock()
	defer t.mu.Unlock()

	buckets, ok := t.keyHits[key]
	if !ok {
		buckets = make(map[int64]int64)
		t.keyHits[key] = buckets
	}
	buckets[now]++
	t.pruneKeyLocked(key, now)
}

func (t *HotKeyTracker) pruneKeyLocked(key string, nowMilli int64) {
	cutoff := nowMilli - t.window.Milliseconds()
	buckets := t.keyHits[key]
	empty := true
	for ms, c := range buckets {
		if ms < cutoff {
			delete(buckets, ms)
		} else if c > 0 {
			empty = false
		}
	}
	if empty {
		delete(t.keyHits, key)
	}
}

// HotKeys returns every key whose count within the window meets or exceeds
// the threshold, sorted by count descending.
func (t *HotKeyTracker) HotKeys() []KeyStat {
	now := time.Now().UnixMilli()
	cutoff := now - t.window.Milliseconds()

	t.mu.Lock()
	defer t.mu.Unlock()

	var result []KeyStat
	for key, buckets := range t.keyHits {
		var total int64
		for ms, c := range buckets {
			if ms >= cutoff {
				total += c
			}
		}
		if total >= t.threshold {
			result = append(result, KeyStat{Key: key, Count: total})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Count != result[j].Count {
			return result[i].Count > result[j].Count
		}
		return result[i].Key < result[j].Key // stable order for equal counts
	})
	return result
}
