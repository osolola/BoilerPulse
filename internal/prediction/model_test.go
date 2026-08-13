package prediction

import (
	"math"
	"testing"
	"time"

	"boilerpulse/internal/events"
)

func trainedModel(t *testing.T) *Model {
	t.Helper()
	m := NewModel()
	m.Train(GenerateSyntheticDataset(2000))
	if !m.Trained() {
		t.Fatal("model did not report Trained() after Train()")
	}
	return m
}

func TestUntrainedModelReturnsZeroOutput(t *testing.T) {
	m := NewModel()
	out := m.Predict(Features{Type: events.TypeDining, StartTime: time.Now()})
	if out != (Output{}) {
		t.Errorf("Predict on untrained model = %+v, want zero value", out)
	}
}

func TestPredictLargeAthleticsEventExceedsSmallDiningEvent(t *testing.T) {
	m := trainedModel(t)

	large := m.Predict(Features{
		Type: events.TypeAthletics, ExpectedAttendance: 14000, Urgency: events.UrgencyHigh,
		StartTime: time.Date(2026, 1, 6, 19, 0, 0, 0, time.UTC), DurationMinutes: 120,
	})
	small := m.Predict(Features{
		Type: events.TypeDining, ExpectedAttendance: 50, Urgency: events.UrgencyNormal,
		StartTime: time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC), DurationMinutes: 60,
	})

	if large.PredictedRPS <= small.PredictedRPS {
		t.Errorf("large-event predicted RPS (%v) should exceed small-event predicted RPS (%v)",
			large.PredictedRPS, small.PredictedRPS)
	}
	if large.RecommendedNodes < small.RecommendedNodes {
		t.Errorf("large-event RecommendedNodes (%d) should be >= small-event's (%d)",
			large.RecommendedNodes, small.RecommendedNodes)
	}
}

func TestPredictCriticalExceedsNormalAtSameAttendance(t *testing.T) {
	m := trainedModel(t)

	critical := m.Predict(Features{
		Type: events.TypeEmergency, ExpectedAttendance: 100, Urgency: events.UrgencyCritical,
		StartTime: time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC), DurationMinutes: 60,
	})
	normal := m.Predict(Features{
		Type: events.TypeStudentOrg, ExpectedAttendance: 100, Urgency: events.UrgencyNormal,
		StartTime: time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC), DurationMinutes: 60,
	})

	if critical.PredictedRPS <= normal.PredictedRPS {
		t.Errorf("CRITICAL predicted RPS (%v) should exceed NORMAL predicted RPS (%v) at equal attendance",
			critical.PredictedRPS, normal.PredictedRPS)
	}
}

func TestPredictNeverNegative(t *testing.T) {
	m := trainedModel(t)
	out := m.Predict(Features{Type: events.TypeSystem, StartTime: time.Date(2026, 1, 6, 3, 0, 0, 0, time.UTC)})
	if out.PredictedRPS < 0 {
		t.Errorf("PredictedRPS = %v, want >= 0", out.PredictedRPS)
	}
}

func TestRecommendedNodesAlwaysAtLeastOne(t *testing.T) {
	m := trainedModel(t)
	out := m.Predict(Features{Type: events.TypeSystem, StartTime: time.Date(2026, 1, 6, 3, 0, 0, 0, time.UTC)})
	if out.RecommendedNodes < 1 {
		t.Errorf("RecommendedNodes = %d, want >= 1", out.RecommendedNodes)
	}
}

func TestPeakTimeIsAfterStartTime(t *testing.T) {
	m := trainedModel(t)
	start := time.Date(2026, 1, 6, 19, 0, 0, 0, time.UTC)
	out := m.Predict(Features{Type: events.TypeAthletics, StartTime: start, DurationMinutes: 120, ExpectedAttendance: 5000})
	if out.PeakTime.Before(start) {
		t.Errorf("PeakTime %v is before StartTime %v", out.PeakTime, start)
	}
}

func TestConfidenceCriticalLowerThanKnownAttendance(t *testing.T) {
	m := trainedModel(t)
	critical := m.Predict(Features{Type: events.TypeEmergency, Urgency: events.UrgencyCritical, StartTime: time.Now()})
	known := m.Predict(Features{Type: events.TypeAthletics, ExpectedAttendance: 5000, Urgency: events.UrgencyHigh, StartTime: time.Now()})

	if critical.Confidence >= known.Confidence {
		t.Errorf("CRITICAL confidence (%v) should be lower than a known-attendance event's confidence (%v)",
			critical.Confidence, known.Confidence)
	}
}

func TestTrainingReducesErrorVersusMeanBaseline(t *testing.T) {
	train := GenerateSyntheticDataset(2000)
	test := GenerateSyntheticDataset(500)

	m := NewModel()
	m.Train(train)

	var meanY float64
	for _, s := range train {
		meanY += s.PeakRPS
	}
	meanY /= float64(len(train))

	var modelSqErr, baselineSqErr float64
	for _, s := range test {
		pred := m.Predict(s.Features).PredictedRPS
		modelSqErr += (pred - s.PeakRPS) * (pred - s.PeakRPS)
		baselineSqErr += (meanY - s.PeakRPS) * (meanY - s.PeakRPS)
	}

	if modelSqErr >= baselineSqErr {
		t.Errorf("trained model's squared error (%v) should be lower than the mean-only baseline's (%v) — model isn't learning anything",
			modelSqErr, baselineSqErr)
	}
}

func TestFeaturesFromEventDefaultsDurationWhenNoEndTime(t *testing.T) {
	e := events.Event{
		Type: events.TypeDining, StartTime: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
	f := FeaturesFromEvent(e)
	if f.DurationMinutes != 60 {
		t.Errorf("DurationMinutes = %v, want 60 (default for missing end_time)", f.DurationMinutes)
	}
}

func TestVectorLengthMatchesNumFeatures(t *testing.T) {
	// Regression guard: numFeatures() and vector() must agree on the
	// feature-vector length, or standardizeParams/gradientDescent panic
	// with an index-out-of-range on whichever one is smaller. This caught
	// a real off-by-one (numFeatures said 4 trailing scalars, vector()
	// actually appends 5) the first time these tests ran.
	f := Features{Type: events.TypeDining, StartTime: time.Now()}
	if got, want := len(f.vector()), numFeatures(); got != want {
		t.Fatalf("len(vector()) = %d, numFeatures() = %d, want equal", got, want)
	}
}

func TestFeaturesFromEventComputesDuration(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e := events.Event{Type: events.TypeAthletics, StartTime: start, EndTime: start.Add(90 * time.Minute)}
	f := FeaturesFromEvent(e)
	if math.Abs(f.DurationMinutes-90) > 0.001 {
		t.Errorf("DurationMinutes = %v, want 90", f.DurationMinutes)
	}
}
