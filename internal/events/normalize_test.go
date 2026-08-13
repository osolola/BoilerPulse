package events

import (
	"errors"
	"testing"
	"time"
)

func validEvent() Event {
	return Event{
		Type:      TypeAthletics,
		Title:     "Purdue Basketball",
		StartTime: time.Now().Add(time.Hour),
	}
}

func TestNormalizeAssignsIDAndCreatedAt(t *testing.T) {
	got, err := Normalize(validEvent())
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.ID == "" {
		t.Error("ID not assigned")
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt not assigned")
	}
}

func TestNormalizePreservesSuppliedID(t *testing.T) {
	e := validEvent()
	e.ID = "evt_fixed"
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.ID != "evt_fixed" {
		t.Errorf("ID = %q, want %q (should not overwrite a supplied ID)", got.ID, "evt_fixed")
	}
}

func TestNormalizeRejectsMissingTitle(t *testing.T) {
	e := validEvent()
	e.Title = ""
	if _, err := Normalize(e); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("Normalize with no title = %v, want ErrInvalidEvent", err)
	}
}

func TestNormalizeRejectsUnknownType(t *testing.T) {
	e := validEvent()
	e.Type = "NOT_A_REAL_TYPE"
	if _, err := Normalize(e); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("Normalize with unknown type = %v, want ErrInvalidEvent", err)
	}
}

func TestNormalizeRejectsMissingStartTime(t *testing.T) {
	e := validEvent()
	e.StartTime = time.Time{}
	if _, err := Normalize(e); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("Normalize with no start_time = %v, want ErrInvalidEvent", err)
	}
}

func TestNormalizeRejectsEndBeforeStart(t *testing.T) {
	e := validEvent()
	e.EndTime = e.StartTime.Add(-time.Hour)
	if _, err := Normalize(e); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("Normalize with end before start = %v, want ErrInvalidEvent", err)
	}
}

func TestNormalizeRejectsNegativeAttendance(t *testing.T) {
	e := validEvent()
	e.ExpectedAttendance = -1
	if _, err := Normalize(e); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("Normalize with negative attendance = %v, want ErrInvalidEvent", err)
	}
}

func TestClassifyEmergencyAndWeatherAreCritical(t *testing.T) {
	for _, typ := range []Type{TypeEmergency, TypeWeather} {
		e := validEvent()
		e.Type = typ
		got, err := Normalize(e)
		if err != nil {
			t.Fatalf("Normalize(%s): %v", typ, err)
		}
		if got.Urgency != UrgencyCritical {
			t.Errorf("Urgency for %s = %q, want %q", typ, got.Urgency, UrgencyCritical)
		}
	}
}

func TestClassifyLargeAthleticsEventIsHigh(t *testing.T) {
	e := validEvent()
	e.ExpectedAttendance = 14000
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Urgency != UrgencyHigh {
		t.Errorf("Urgency = %q, want %q", got.Urgency, UrgencyHigh)
	}
}

func TestClassifySmallAthleticsEventIsNormal(t *testing.T) {
	e := validEvent()
	e.ExpectedAttendance = 50
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Urgency != UrgencyNormal {
		t.Errorf("Urgency = %q, want %q", got.Urgency, UrgencyNormal)
	}
}

func TestClassifyDoesNotOverrideSuppliedUrgency(t *testing.T) {
	e := validEvent()
	e.Type = TypeDining // would normally classify as NORMAL
	e.Urgency = UrgencyHigh
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Urgency != UrgencyHigh {
		t.Errorf("Urgency = %q, want the caller-supplied %q preserved", got.Urgency, UrgencyHigh)
	}
}

func TestEstimateTrafficCriticalGetsHighMultiplier(t *testing.T) {
	e := validEvent()
	e.Type = TypeEmergency
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.ExpectedTrafficMultiplier < 10 {
		t.Errorf("ExpectedTrafficMultiplier = %v, want a high multiplier for a CRITICAL event", got.ExpectedTrafficMultiplier)
	}
}

func TestEstimateTrafficScalesWithAttendance(t *testing.T) {
	small := validEvent()
	small.ExpectedAttendance = 50
	large := validEvent()
	large.ExpectedAttendance = 14000

	gotSmall, err := Normalize(small)
	if err != nil {
		t.Fatalf("Normalize(small): %v", err)
	}
	gotLarge, err := Normalize(large)
	if err != nil {
		t.Fatalf("Normalize(large): %v", err)
	}
	if gotLarge.ExpectedTrafficMultiplier <= gotSmall.ExpectedTrafficMultiplier {
		t.Errorf("large event multiplier %v should exceed small event multiplier %v",
			gotLarge.ExpectedTrafficMultiplier, gotSmall.ExpectedTrafficMultiplier)
	}
}

func TestEstimateTrafficDoesNotOverrideSuppliedValue(t *testing.T) {
	e := validEvent()
	e.ExpectedTrafficMultiplier = 42
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.ExpectedTrafficMultiplier != 42 {
		t.Errorf("ExpectedTrafficMultiplier = %v, want the caller-supplied 42 preserved", got.ExpectedTrafficMultiplier)
	}
}

func TestConfidenceForSimulatorIsFull(t *testing.T) {
	e := validEvent()
	e.Source = "SIMULATOR"
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Confidence != 1.0 {
		t.Errorf("Confidence = %v, want 1.0 for SIMULATOR source", got.Confidence)
	}
}

func TestConfidenceForOtherSourceIsLower(t *testing.T) {
	e := validEvent()
	e.Source = "SOME_EXTERNAL_FEED"
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got.Confidence >= 1.0 {
		t.Errorf("Confidence = %v, want < 1.0 for a non-simulator source", got.Confidence)
	}
}
