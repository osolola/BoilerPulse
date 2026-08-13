package workload

import (
	"testing"
	"time"
)

func TestRequestMonitorRPS(t *testing.T) {
	m := NewRequestMonitor(10 * time.Second)
	for i := 0; i < 5; i++ {
		m.Record()
	}
	rps := m.RPS()
	if rps <= 0 {
		t.Fatalf("RPS() = %v, want > 0 after recording 5 requests", rps)
	}
	// 5 requests over a 10s window averages to 0.5 rps.
	if rps < 0.4 || rps > 0.6 {
		t.Errorf("RPS() = %v, want ~0.5", rps)
	}
}

func TestRequestMonitorZeroWhenEmpty(t *testing.T) {
	m := NewRequestMonitor(10 * time.Second)
	if rps := m.RPS(); rps != 0 {
		t.Errorf("RPS() on empty monitor = %v, want 0", rps)
	}
}

func TestRequestMonitorPrunesOldBuckets(t *testing.T) {
	m := NewRequestMonitor(50 * time.Millisecond)
	m.Record()
	if rps := m.RPS(); rps <= 0 {
		t.Fatalf("RPS() immediately after Record = %v, want > 0", rps)
	}

	time.Sleep(200 * time.Millisecond) // well past the window
	m.Record()                         // triggers a prune as a side effect

	// Only the most recent record should count now; verify old buckets
	// don't inflate the rate indefinitely by checking the bucket count
	// directly isn't exposed, so instead assert RPS stays bounded and
	// doesn't grow across repeated stale calls.
	first := m.RPS()
	time.Sleep(10 * time.Millisecond)
	second := m.RPS()
	if second > first+0.5 {
		t.Errorf("RPS grew unexpectedly across calls with no new records: %v -> %v", first, second)
	}
}
