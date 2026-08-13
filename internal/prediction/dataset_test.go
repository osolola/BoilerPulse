package prediction

import (
	"testing"
	"time"

	"boilerpulse/internal/events"
)

func fixedNoonMonday() time.Time {
	return time.Date(2026, time.January, 5, 12, 0, 0, 0, time.UTC) // a Monday, per dataset.go's comment
}

func isKnownEventType(want events.Type) bool {
	for _, et := range eventTypes {
		if et == want {
			return true
		}
	}
	return false
}

func TestGenerateSyntheticDatasetProducesValidSamples(t *testing.T) {
	samples := GenerateSyntheticDataset(500)
	if len(samples) != 500 {
		t.Fatalf("got %d samples, want 500", len(samples))
	}

	seenTypes := make(map[string]bool)
	for _, s := range samples {
		if s.PeakRPS <= 0 {
			t.Errorf("sample PeakRPS = %v, want > 0", s.PeakRPS)
		}
		if !isKnownEventType(s.Features.Type) {
			t.Errorf("sample has invalid event type %q", s.Features.Type)
		}
		seenTypes[string(s.Features.Type)] = true
	}
	if len(seenTypes) < 2 {
		t.Errorf("dataset only produced %d distinct event types across 500 samples, want more variety", len(seenTypes))
	}
}

func TestGroundTruthRPSScalesWithUrgency(t *testing.T) {
	base := Features{ExpectedAttendance: 100, StartTime: fixedNoonMonday()}
	critical := base
	critical.Urgency = "CRITICAL"
	normal := base
	normal.Urgency = "NORMAL"

	if groundTruthRPS(critical) <= groundTruthRPS(normal) {
		t.Errorf("groundTruthRPS(CRITICAL) = %v, want > groundTruthRPS(NORMAL) = %v",
			groundTruthRPS(critical), groundTruthRPS(normal))
	}
}
