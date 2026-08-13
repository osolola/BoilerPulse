package workload

import (
	"testing"
	"time"
)

func testThresholds() Thresholds {
	return Thresholds{ElevatedRPS: 2, HighTrafficRPS: 5, CriticalRPS: 10}
}

func TestEngineStartsNormal(t *testing.T) {
	e := NewEngine(testThresholds())
	if e.Mode() != ModeNormal {
		t.Errorf("initial Mode() = %q, want %q", e.Mode(), ModeNormal)
	}
}

func TestEngineEscalatesWithRequestRate(t *testing.T) {
	e := NewEngine(testThresholds())

	// Push RPS above ElevatedRPS (2) but below HighTrafficRPS (5): with a
	// 10s window, ~25 requests averages to 2.5 rps.
	for i := 0; i < 25; i++ {
		e.RecordRequest("")
	}
	if mode := e.Mode(); mode != ModeElevated {
		t.Errorf("Mode() after moderate burst = %q, want %q (rps=%v)", mode, ModeElevated, e.RPS())
	}
}

func TestEngineEscalatesToHighTraffic(t *testing.T) {
	e := NewEngine(testThresholds())
	for i := 0; i < 60; i++ { // ~6 rps over a 10s window
		e.RecordRequest("")
	}
	if mode := e.Mode(); mode != ModeHighTraffic {
		t.Errorf("Mode() after large burst = %q, want %q (rps=%v)", mode, ModeHighTraffic, e.RPS())
	}
}

func TestEngineDeEscalatesWhenTrafficDrops(t *testing.T) {
	e := NewEngine(Thresholds{ElevatedRPS: 1000, HighTrafficRPS: 2000, CriticalRPS: 5000})
	// With enormous thresholds, a handful of requests never elevates.
	e.RecordRequest("")
	if mode := e.Mode(); mode != ModeNormal {
		t.Errorf("Mode() = %q, want %q (mode should be reactive, not sticky)", mode, ModeNormal)
	}
}

func TestSignalCriticalForcesModeRegardlessOfRPS(t *testing.T) {
	e := NewEngine(Thresholds{ElevatedRPS: 1000, HighTrafficRPS: 2000, CriticalRPS: 5000})
	e.SignalCritical(50 * time.Millisecond)
	if mode := e.Mode(); mode != ModeCritical {
		t.Fatalf("Mode() after SignalCritical = %q, want %q", mode, ModeCritical)
	}

	// A subsequent low-traffic request should NOT immediately clear
	// CRITICAL -- the hold applies even as RecordRequest recomputes mode.
	e.RecordRequest("")
	if mode := e.Mode(); mode != ModeCritical {
		t.Errorf("Mode() = %q, want %q to still hold", mode, ModeCritical)
	}
}

func TestCriticalSignalExpiresAfterHold(t *testing.T) {
	e := NewEngine(Thresholds{ElevatedRPS: 1000, HighTrafficRPS: 2000, CriticalRPS: 5000})
	e.SignalCritical(20 * time.Millisecond)
	if e.Mode() != ModeCritical {
		t.Fatalf("Mode() = %q, want %q immediately after signal", e.Mode(), ModeCritical)
	}

	time.Sleep(60 * time.Millisecond)
	e.RecordRequest("") // triggers a mode recompute
	if mode := e.Mode(); mode != ModeNormal {
		t.Errorf("Mode() after hold expiry = %q, want %q", mode, ModeNormal)
	}
}

func TestEngineStatusReflectsHotKeys(t *testing.T) {
	e := NewEngine(testThresholds())
	for i := 0; i < 25; i++ {
		e.RecordRequest("event:mackey")
	}

	status := e.Status()
	if len(status.HotKeys) != 1 || status.HotKeys[0].Key != "event:mackey" {
		t.Errorf("Status().HotKeys = %+v, want event:mackey listed", status.HotKeys)
	}
	if status.Mode != e.Mode() {
		t.Errorf("Status().Mode = %q, want %q", status.Mode, e.Mode())
	}
}
