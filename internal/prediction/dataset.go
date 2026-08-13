package prediction

import (
	"math/rand"
	"time"

	"boilerpulse/internal/events"
)

// Sample is one labeled training example: features plus an observed peak RPS.
type Sample struct {
	Features Features
	PeakRPS  float64
}

// GenerateSyntheticDataset produces n labeled samples from a documented
// ground-truth formula (groundTruthRPS) plus noise. This is explicitly NOT
// real Purdue traffic data — spec §67-C requires that be stated plainly,
// not implied. It stands in for what the full traffic simulator (§10-11,
// Milestone 10's cmd/simulator) would otherwise produce by actually running
// many synthetic semesters and observing resulting RPS curves; generating
// directly from a formula is a lighter-weight substitute sufficient to
// train and validate a small regression model without that generator
// existing yet.
func GenerateSyntheticDataset(n int) []Sample {
	samples := make([]Sample, 0, n)
	for i := 0; i < n; i++ {
		f := randomFeatures()
		truth := groundTruthRPS(f)
		noisy := truth + rand.NormFloat64()*truth*0.1 // +/-10%-ish noise
		if noisy < 1 {
			noisy = 1
		}
		samples = append(samples, Sample{Features: f, PeakRPS: noisy})
	}
	return samples
}

func randomFeatures() Features {
	typ := eventTypes[rand.Intn(len(eventTypes))]

	var attendance int
	switch typ {
	case events.TypeAthletics, events.TypeCampusEvent:
		attendance = rand.Intn(15000)
	case events.TypeStudentOrg, events.TypeDining:
		attendance = rand.Intn(500)
	default:
		attendance = rand.Intn(50)
	}

	var urgency events.Urgency
	switch typ {
	case events.TypeEmergency, events.TypeWeather:
		urgency = events.UrgencyCritical
	case events.TypeAthletics, events.TypeCampusEvent:
		if attendance >= 5000 {
			urgency = events.UrgencyHigh
		} else {
			urgency = events.UrgencyNormal
		}
	default:
		urgency = events.UrgencyNormal
	}

	hour := rand.Intn(24)
	dayOffset := rand.Intn(7)
	start := time.Date(2026, time.January, 5+dayOffset, hour, 0, 0, 0, time.UTC) // Jan 5 2026 is a Monday
	duration := float64(30 + rand.Intn(180))

	return Features{Type: typ, ExpectedAttendance: attendance, Urgency: urgency, StartTime: start, DurationMinutes: duration}
}

// groundTruthRPS is the synthetic "real" relationship the model is trained
// to approximate: a baseline scaled by urgency and attendance, with an
// evening-peak bump. Loosely mirrors the spec's own example scenario
// magnitudes (§10: normal ~500rps, athletics ~15000rps, emergency
// ~50000rps) without claiming to reproduce them exactly.
func groundTruthRPS(f Features) float64 {
	base := 500.0 + float64(f.ExpectedAttendance)
	base *= urgencyMultiplier(f.Urgency)

	hour := f.StartTime.Hour()
	if hour >= 17 && hour <= 21 {
		base *= 1.3
	}
	return base
}

func urgencyMultiplier(u events.Urgency) float64 {
	switch u {
	case events.UrgencyCritical:
		return 30
	case events.UrgencyScheduledSpike:
		return 8
	case events.UrgencyHigh:
		return 4
	default:
		return 1
	}
}
