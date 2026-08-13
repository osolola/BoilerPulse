// Package simulator generates real HTTP load against a running BoilerPulse
// gateway, following named traffic-curve scenarios (spec §10/§57 Milestone
// 10: normal/finals/athletics/emergency/hot-key/failure), and measures what
// actually happened — achieved RPS, latency percentiles, and error rate.
// Nothing here estimates or invents numbers; see cmd/simulator for the CLI
// that runs a scenario and benchmarks/ for real recorded results.
package simulator

import (
	"fmt"
	"math/rand"
	"time"
)

// Scenario describes one named traffic pattern: a baseline→peak→baseline
// RPS curve (linear ramps), a read/write mix, and how concentrated
// requests are on a small "hot" set of keys vs. spread across the whole
// key space.
type Scenario struct {
	Name        string
	Description string

	BaselineRPS  float64
	PeakRPS      float64
	RampUp       time.Duration
	PeakDuration time.Duration
	RampDown     time.Duration

	// WriteRatio is the fraction (0-1) of requests that are PUT rather
	// than GET.
	WriteRatio float64
	// HotKeyFraction is the fraction (0-1) of requests that hit one of
	// HotKeyCount "hot" keys rather than a uniformly random key from the
	// full KeySpace -- simulating everyone looking at the same popular
	// event.
	HotKeyFraction float64
	HotKeyCount    int
	KeySpace       int
}

// TotalDuration is the full run length: ramp-up + peak + ramp-down.
func (s Scenario) TotalDuration() time.Duration {
	return s.RampUp + s.PeakDuration + s.RampDown
}

// TargetRPS returns the target request rate at elapsed time t into the
// run: a linear ramp from BaselineRPS to PeakRPS over RampUp, a plateau at
// PeakRPS for PeakDuration, then a linear ramp back down to BaselineRPS
// over RampDown. Before 0 or after TotalDuration, it holds at BaselineRPS.
func (s Scenario) TargetRPS(elapsed time.Duration) float64 {
	switch {
	case elapsed < 0:
		return s.BaselineRPS
	case elapsed < s.RampUp:
		if s.RampUp <= 0 {
			return s.PeakRPS
		}
		frac := float64(elapsed) / float64(s.RampUp)
		return s.BaselineRPS + frac*(s.PeakRPS-s.BaselineRPS)
	case elapsed < s.RampUp+s.PeakDuration:
		return s.PeakRPS
	case elapsed < s.TotalDuration():
		rampElapsed := elapsed - s.RampUp - s.PeakDuration
		if s.RampDown <= 0 {
			return s.BaselineRPS
		}
		frac := float64(rampElapsed) / float64(s.RampDown)
		return s.PeakRPS - frac*(s.PeakRPS-s.BaselineRPS)
	default:
		return s.BaselineRPS
	}
}

// pickOp decides GET vs PUT for one request.
func (s Scenario) pickOp(rng *rand.Rand) string {
	if rng.Float64() < s.WriteRatio {
		return "PUT"
	}
	return "GET"
}

// pickKey decides which key one request targets, per HotKeyFraction.
func (s Scenario) pickKey(rng *rand.Rand) string {
	if s.HotKeyCount > 0 && rng.Float64() < s.HotKeyFraction {
		return fmt.Sprintf("sim:hot:%d", rng.Intn(s.HotKeyCount))
	}
	return fmt.Sprintf("sim:key:%d", rng.Intn(s.KeySpace))
}

// allKeys returns every key this scenario might touch, for warmup priming
// (see Generator.Run) -- so timed GETs hit real data instead of a mix of
// 404s from an empty key space.
func (s Scenario) allKeys() []string {
	keys := make([]string, 0, s.HotKeyCount+s.KeySpace)
	for i := 0; i < s.HotKeyCount; i++ {
		keys = append(keys, fmt.Sprintf("sim:hot:%d", i))
	}
	for i := 0; i < s.KeySpace; i++ {
		keys = append(keys, fmt.Sprintf("sim:key:%d", i))
	}
	return keys
}

// Normal is a typical weekday's steady, unremarkable campus traffic.
func Normal() Scenario {
	return Scenario{
		Name:           "normal",
		Description:    "Typical weekday campus traffic: steady baseline, no spikes.",
		BaselineRPS:    10,
		PeakRPS:        10,
		PeakDuration:   20 * time.Second,
		WriteRatio:     0.2,
		HotKeyFraction: 0.1,
		HotKeyCount:    5,
		KeySpace:       500,
	}
}

// Finals is finals-week traffic: elevated and sustained, not spiky.
func Finals() Scenario {
	return Scenario{
		Name:           "finals",
		Description:    "Finals week: elevated sustained load, study-room/library lookups dominate.",
		BaselineRPS:    10,
		PeakRPS:        60,
		RampUp:         5 * time.Second,
		PeakDuration:   20 * time.Second,
		RampDown:       5 * time.Second,
		WriteRatio:     0.15,
		HotKeyFraction: 0.3,
		HotKeyCount:    10,
		KeySpace:       500,
	}
}

// Athletics is a home-game traffic pattern: a sharp ramp to a high,
// sustained peak concentrated on a handful of popular keys (the game's own
// event page).
func Athletics() Scenario {
	return Scenario{
		Name:           "athletics",
		Description:    "Home game: sharp ramp to a high sustained peak around one popular event.",
		BaselineRPS:    10,
		PeakRPS:        150,
		RampUp:         5 * time.Second,
		PeakDuration:   20 * time.Second,
		RampDown:       10 * time.Second,
		WriteRatio:     0.1,
		HotKeyFraction: 0.7,
		HotKeyCount:    3,
		KeySpace:       500,
	}
}

// Emergency is an emergency-alert traffic pattern: near-instant spike, a
// higher write ratio (status updates), almost everyone hitting the same
// alert key.
func Emergency() Scenario {
	return Scenario{
		Name:           "emergency",
		Description:    "Emergency alert: near-instant spike to peak, elevated write ratio for status updates.",
		BaselineRPS:    10,
		PeakRPS:        200,
		RampUp:         1 * time.Second,
		PeakDuration:   15 * time.Second,
		RampDown:       5 * time.Second,
		WriteRatio:     0.4,
		HotKeyFraction: 0.9,
		HotKeyCount:    1,
		KeySpace:       500,
	}
}

// HotKey isolates hot-key contention specifically: moderate steady RPS,
// almost every request hitting a single key, to see the effect in
// isolation from any ramp.
func HotKey() Scenario {
	return Scenario{
		Name:           "hotkey",
		Description:    "Isolated hot-key stress: moderate steady RPS, nearly all requests hit one key.",
		BaselineRPS:    30,
		PeakRPS:        30,
		PeakDuration:   20 * time.Second,
		WriteRatio:     0.05,
		HotKeyFraction: 0.95,
		HotKeyCount:    1,
		KeySpace:       500,
	}
}

// All returns every named scenario, in a stable order.
func All() []Scenario {
	return []Scenario{Normal(), Finals(), Athletics(), Emergency(), HotKey()}
}

// ByName looks up a scenario by its Name field.
func ByName(name string) (Scenario, bool) {
	for _, s := range All() {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}
