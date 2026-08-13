package events

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidEvent wraps every validation failure Normalize can return.
var ErrInvalidEvent = errors.New("events: invalid event")

// Normalize runs the spec's pipeline (§9): validate, assign an ID and
// CreatedAt if missing, classify urgency if the source didn't already
// supply one, and estimate a traffic multiplier if the source didn't
// supply one. It never mutates fields a trusted source already set —
// classification and traffic estimation only fill in zero values.
func Normalize(raw Event) (Event, error) {
	if err := validate(raw); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}

	e := raw
	if e.ID == "" {
		e.ID = generateID()
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}

	classify(&e)
	estimateTraffic(&e)
	e.Confidence = confidenceFor(e)

	return e, nil
}

func validate(e Event) error {
	if e.Title == "" {
		return errors.New("title is required")
	}
	if !e.Type.valid() {
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	if e.StartTime.IsZero() {
		return errors.New("start_time is required")
	}
	if !e.EndTime.IsZero() && e.EndTime.Before(e.StartTime) {
		return errors.New("end_time must not be before start_time")
	}
	if e.ExpectedAttendance < 0 {
		return errors.New("expected_attendance must not be negative")
	}
	return nil
}

// classify assigns Urgency from Type/attendance when the source didn't
// already provide one. EMERGENCY and WEATHER map straight to CRITICAL —
// these are exactly the "emergency alert, severe weather" cases the spec's
// workload model (§5) treats as bypassing normal traffic ramp-up.
func classify(e *Event) {
	if e.Urgency != "" {
		return
	}
	switch e.Type {
	case TypeEmergency, TypeWeather:
		e.Urgency = UrgencyCritical
	case TypeAthletics:
		if e.ExpectedAttendance >= 5000 {
			e.Urgency = UrgencyHigh
		} else {
			e.Urgency = UrgencyNormal
		}
	case TypeCampusEvent:
		if e.ExpectedAttendance >= 2000 {
			e.Urgency = UrgencyHigh
		} else {
			e.Urgency = UrgencyNormal
		}
	default:
		e.Urgency = UrgencyNormal
	}
}

// estimateTraffic assigns a rough per-event traffic multiplier when the
// source didn't already provide one. This is a simple heuristic for a
// single event at ingestion time, not the trained prediction model
// (Milestone 7, internal/prediction) — that operates on synthetic
// historical data across many events, not one event in isolation.
func estimateTraffic(e *Event) {
	if e.ExpectedTrafficMultiplier > 0 {
		return
	}
	switch {
	case e.Urgency == UrgencyCritical:
		e.ExpectedTrafficMultiplier = 20.0
	case e.ExpectedAttendance > 0:
		m := 1.0 + float64(e.ExpectedAttendance)/1000.0
		if m > 15 {
			m = 15
		}
		e.ExpectedTrafficMultiplier = m
	default:
		e.ExpectedTrafficMultiplier = 1.0
	}
}

// confidenceFor is a placeholder trust score: full confidence for the
// simulator (there's nothing to doubt, it's synthetic by definition), lower
// for anything else — a hook for when lower-trust external sources
// (§40: "public event/calendar data") are added later.
func confidenceFor(e Event) float64 {
	if e.Source == "SIMULATOR" {
		return 1.0
	}
	return 0.7
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "evt_" + hex.EncodeToString(b)
}
