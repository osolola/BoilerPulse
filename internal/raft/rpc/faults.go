package rpc

import (
	"math/rand"
	"sync"
	"time"
)

// Faults holds one node's currently-injected chaos state (spec §23: "delay
// network, drop packets, partition network"). The same *Faults instance is
// shared by Transport (checked on outgoing RPCs) and Server (checked on
// incoming RPCs), so setting Partitioned on a node genuinely isolates it in
// both directions without needing to coordinate with peers.
//
// This exists purely for demo/chaos-testing (spec §34) — see
// internal/admin, which mounts the controls on a separate, token-gated
// port, and docs/failure-testing.md.
type Faults struct {
	mu          sync.RWMutex
	partitioned bool
	latency     time.Duration
	dropRate    float64 // 0.0-1.0
}

// NewFaults returns a Faults with nothing injected.
func NewFaults() *Faults {
	return &Faults{}
}

func (f *Faults) SetPartitioned(p bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitioned = p
}

func (f *Faults) SetLatency(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latency = d
}

func (f *Faults) SetDropRate(rate float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	f.dropRate = rate
}

// Reset clears every injected fault.
func (f *Faults) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitioned = false
	f.latency = 0
	f.dropRate = 0
}

// Status is a snapshot of currently-injected faults, for the admin status
// endpoint. latency is unexported (so excluded from JSON automatically) —
// LatencyMS is what's actually serialized.
type Status struct {
	Partitioned bool    `json:"partitioned"`
	LatencyMS   int64   `json:"latency_ms"`
	DropRate    float64 `json:"drop_rate"`
	latency     time.Duration
}

func (f *Faults) Status() Status {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return Status{Partitioned: f.partitioned, LatencyMS: f.latency.Milliseconds(), DropRate: f.dropRate, latency: f.latency}
}

// apply blocks for the configured latency (if any) and reports whether the
// caller should proceed — false means "drop this," whether because the
// node is partitioned or a random drop-rate roll hit.
func (f *Faults) apply() bool {
	status := f.Status()
	if status.Partitioned {
		return false
	}
	if status.latency > 0 {
		time.Sleep(status.latency)
	}
	if status.DropRate > 0 && rand.Float64() < status.DropRate {
		return false
	}
	return true
}
