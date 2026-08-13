package simulator

import (
	"encoding/json"
	"fmt"
	"time"
)

// Report is what actually happened during a Generator.Run — real measured
// numbers, never estimated or invented (spec's benchmarking honesty
// requirement, same as internal/prediction's synthetic-data labeling).
type Report struct {
	Scenario      string         `json:"scenario"`
	Topology      string         `json:"topology"`
	StartedAt     time.Time      `json:"started_at"`
	DurationMS    int64          `json:"duration_ms"`
	TotalRequests int            `json:"total_requests"`
	ErrorCount    int            `json:"error_count"`
	ErrorRate     float64        `json:"error_rate"`
	AchievedRPS   float64        `json:"achieved_rps"`
	LatencyP50Ms  float64        `json:"latency_p50_ms"`
	LatencyP95Ms  float64        `json:"latency_p95_ms"`
	LatencyP99Ms  float64        `json:"latency_p99_ms"`
	LatencyMaxMs  float64        `json:"latency_max_ms"`
	StatusCounts  map[string]int `json:"status_counts"`
	Notes         []string       `json:"notes,omitempty"`
}

// JSON renders the report as indented JSON.
func (r Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// Summary renders a one-line human-readable summary, e.g. for CLI output.
func (r Report) Summary() string {
	return fmt.Sprintf(
		"%s/%s: %d requests over %.1fs (%.1f rps achieved), %.2f%% errors, latency p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms",
		r.Scenario, r.Topology, r.TotalRequests, float64(r.DurationMS)/1000, r.AchievedRPS,
		r.ErrorRate*100, r.LatencyP50Ms, r.LatencyP95Ms, r.LatencyP99Ms, r.LatencyMaxMs,
	)
}
