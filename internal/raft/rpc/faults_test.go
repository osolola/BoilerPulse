package rpc

import (
	"context"
	"testing"
	"time"

	"boilerpulse/internal/raft"
)

func TestFaultsDefaultAllowsThrough(t *testing.T) {
	f := NewFaults()
	if !f.apply() {
		t.Error("apply() = false with no faults set, want true")
	}
}

func TestFaultsPartitionBlocks(t *testing.T) {
	f := NewFaults()
	f.SetPartitioned(true)
	if f.apply() {
		t.Error("apply() = true while partitioned, want false")
	}
}

func TestFaultsRestoreClearsPartition(t *testing.T) {
	f := NewFaults()
	f.SetPartitioned(true)
	f.Reset()
	if !f.apply() {
		t.Error("apply() = false after Reset, want true")
	}
}

func TestFaultsLatencyDelaysApply(t *testing.T) {
	f := NewFaults()
	f.SetLatency(30 * time.Millisecond)

	start := time.Now()
	f.apply()
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("apply() returned after %v, want >= 30ms", elapsed)
	}
}

func TestFaultsFullDropRateAlwaysBlocks(t *testing.T) {
	f := NewFaults()
	f.SetDropRate(1.0)
	for i := 0; i < 20; i++ {
		if f.apply() {
			t.Fatal("apply() = true with drop_rate=1.0, want always false")
		}
	}
}

func TestFaultsZeroDropRateNeverBlocks(t *testing.T) {
	f := NewFaults()
	f.SetDropRate(0)
	for i := 0; i < 20; i++ {
		if !f.apply() {
			t.Fatal("apply() = false with drop_rate=0, want always true")
		}
	}
}

func TestFaultsDropRateClampedToValidRange(t *testing.T) {
	f := NewFaults()
	f.SetDropRate(5) // out of range, should clamp to 1
	if f.Status().DropRate != 1 {
		t.Errorf("DropRate = %v, want clamped to 1", f.Status().DropRate)
	}

	f.SetDropRate(-1) // out of range, should clamp to 0
	if f.Status().DropRate != 0 {
		t.Errorf("DropRate = %v, want clamped to 0", f.Status().DropRate)
	}
}

func TestFaultsStatusReflectsCurrentState(t *testing.T) {
	f := NewFaults()
	f.SetPartitioned(true)
	f.SetLatency(75 * time.Millisecond)
	f.SetDropRate(0.25)

	status := f.Status()
	if !status.Partitioned {
		t.Error("Status().Partitioned = false, want true")
	}
	if status.LatencyMS != 75 {
		t.Errorf("Status().LatencyMS = %d, want 75", status.LatencyMS)
	}
	if status.DropRate != 0.25 {
		t.Errorf("Status().DropRate = %v, want 0.25", status.DropRate)
	}
}

func TestTransportWithNilFaultsBehavesNormally(t *testing.T) {
	// A Transport constructed with faults=nil must never panic or block --
	// production nodes without chaos wired in rely on this.
	tr := NewTransport(map[string]string{}, nil)
	_, err := tr.SendRequestVote(context.Background(), "unknown-peer", &raft.RequestVoteArgs{Term: 1})
	if err == nil {
		t.Error("SendRequestVote to an unconfigured peer returned nil error, want an address-resolution error")
	}
}
