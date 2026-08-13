package workload

import (
	"sync"
	"time"
)

// Engine combines request-rate monitoring, hot-key detection, and the
// workload-mode state machine. Feed it every request via RecordRequest;
// read Mode/Status whenever you need the current state (e.g. per-request,
// or for a status endpoint).
type Engine struct {
	requests *RequestMonitor
	hotKeys  *HotKeyTracker

	thresholds Thresholds

	mu            sync.Mutex
	mode          Mode
	criticalUntil time.Time
}

// NewEngine builds an Engine with a 10s tracking window and a hot-key
// threshold of 20 requests within that window.
func NewEngine(thresholds Thresholds) *Engine {
	return &Engine{
		requests:   NewRequestMonitor(10 * time.Second),
		hotKeys:    NewHotKeyTracker(10*time.Second, 20),
		thresholds: thresholds,
		mode:       ModeNormal,
	}
}

// RecordRequest counts one request, optionally against a specific key (pass
// "" if the request isn't for a single addressable key, e.g. a list
// endpoint), and recomputes the current mode.
func (e *Engine) RecordRequest(key string) {
	e.requests.Record()
	if key != "" {
		e.hotKeys.Record(key)
	}
	e.recomputeMode()
}

// SignalCritical forces CRITICAL mode for at least `hold`, regardless of
// current request rate — per spec §26, "Emergency events can directly enter
// CRITICAL." Calling it again extends the hold if the new expiry is later.
func (e *Engine) SignalCritical(hold time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	until := time.Now().Add(hold)
	if until.After(e.criticalUntil) {
		e.criticalUntil = until
	}
	e.mode = ModeCritical
}

func (e *Engine) recomputeMode() {
	rps := e.requests.RPS()

	e.mu.Lock()
	defer e.mu.Unlock()

	if time.Now().Before(e.criticalUntil) {
		e.mode = ModeCritical
		return
	}

	switch {
	case rps >= e.thresholds.CriticalRPS:
		e.mode = ModeCritical
	case rps >= e.thresholds.HighTrafficRPS:
		e.mode = ModeHighTraffic
	case rps >= e.thresholds.ElevatedRPS:
		e.mode = ModeElevated
	default:
		e.mode = ModeNormal
	}
}

// Mode returns the current workload mode.
func (e *Engine) Mode() Mode {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mode
}

// RPS returns the current requests-per-second average.
func (e *Engine) RPS() float64 {
	return e.requests.RPS()
}

// HotKeys returns keys currently over the hot-key threshold.
func (e *Engine) HotKeys() []KeyStat {
	return e.hotKeys.HotKeys()
}

// Status is a point-in-time snapshot of everything the Engine tracks.
type Status struct {
	Mode    Mode      `json:"mode"`
	RPS     float64   `json:"rps"`
	HotKeys []KeyStat `json:"hot_keys,omitempty"`
}

func (e *Engine) Status() Status {
	return Status{Mode: e.Mode(), RPS: e.RPS(), HotKeys: e.HotKeys()}
}
