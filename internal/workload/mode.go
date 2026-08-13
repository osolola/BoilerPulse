// Package workload implements the spec's automatic workload-mode state
// machine (§26): NORMAL/ELEVATED/HIGH_TRAFFIC/CRITICAL, driven by request
// rate and explicit CRITICAL signals (emergency events), plus hot-key
// detection (§12). It has no dependency on HTTP or the KV store — the
// gateway is what feeds it request/key observations and reads its state.
package workload

// Mode is one of the spec's four workload classes.
type Mode string

const (
	ModeNormal      Mode = "NORMAL"
	ModeElevated    Mode = "ELEVATED"
	ModeHighTraffic Mode = "HIGH_TRAFFIC"
	ModeCritical    Mode = "CRITICAL"
)

// Thresholds are the requests-per-second boundaries between modes. Mode
// selection is purely reactive to current RPS (no hysteresis/cooldown on
// the way down) — a deliberate simplification; see docs/workload-model.md.
type Thresholds struct {
	ElevatedRPS    float64
	HighTrafficRPS float64
	CriticalRPS    float64
}

// DefaultThresholds are sized for local demo traffic, not the spec's
// full-scale scenario numbers (§10-11) — those are for the load simulator
// (Milestone 10), which can pass its own Thresholds.
func DefaultThresholds() Thresholds {
	return Thresholds{
		ElevatedRPS:    20,
		HighTrafficRPS: 100,
		CriticalRPS:    500,
	}
}
