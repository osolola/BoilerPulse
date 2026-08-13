package workload

import (
	"sync"
	"time"
)

// RequestMonitor tracks requests-per-second over a sliding time window
// using millisecond-granularity buckets (not whole-second buckets: at
// second granularity, int64(window.Seconds()) truncates any sub-second
// window to a 0 cutoff, silently breaking the slide — millisecond
// granularity makes the math correct at any window size, including the
// short windows tests use for speed). Simple and correct at demo scale; a
// higher-throughput system would use a lock-free ring buffer instead of a
// mutex-guarded map.
type RequestMonitor struct {
	mu      sync.Mutex
	window  time.Duration
	buckets map[int64]int64 // unix milli -> request count
}

// NewRequestMonitor returns a monitor tracking the last `window` of requests.
func NewRequestMonitor(window time.Duration) *RequestMonitor {
	return &RequestMonitor{window: window, buckets: make(map[int64]int64)}
}

// Record counts one request at the current time.
func (m *RequestMonitor) Record() {
	now := time.Now().UnixMilli()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets[now]++
	m.pruneLocked(now)
}

// RPS returns the average requests-per-second over the trailing window.
func (m *RequestMonitor) RPS() float64 {
	now := time.Now().UnixMilli()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(now)

	var total int64
	for _, c := range m.buckets {
		total += c
	}
	windowSecs := m.window.Seconds()
	if windowSecs <= 0 {
		return 0
	}
	return float64(total) / windowSecs
}

func (m *RequestMonitor) pruneLocked(nowMilli int64) {
	cutoff := nowMilli - m.window.Milliseconds()
	for ms := range m.buckets {
		if ms < cutoff {
			delete(m.buckets, ms)
		}
	}
}
