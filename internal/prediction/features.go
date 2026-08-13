// Package prediction implements the spec's traffic prediction component
// (§28-29): a small linear-regression model trained on a synthetic dataset
// (§67-C — there's no real historical Purdue traffic to train on, and the
// spec is explicit that this must be documented rather than implied to be
// real), producing predicted RPS / confidence / peak time / recommended
// node count for a given event.
package prediction

import (
	"time"

	"boilerpulse/internal/events"
)

// eventTypes is the fixed category set used for one-hot encoding — order
// matters (it defines vector layout) but not meaning.
var eventTypes = []events.Type{
	events.TypeAcademic, events.TypeAthletics, events.TypeStudentOrg, events.TypeCampusEvent,
	events.TypeDining, events.TypeTransportation, events.TypeWeather, events.TypeEmergency, events.TypeSystem,
}

// Features are the inputs the model predicts from — deliberately the
// subset of the spec's suggested feature list (§28) that a normalized
// Event actually carries: type, expected attendance, urgency, start time
// (which gives hour-of-day and day-of-week), and duration. Geographic
// concentration and "historical traffic" as a feature are not implemented —
// see docs/prediction.md.
type Features struct {
	Type               events.Type
	ExpectedAttendance int
	Urgency            events.Urgency
	StartTime          time.Time
	DurationMinutes    float64
}

// FeaturesFromEvent extracts Features from a normalized event.
func FeaturesFromEvent(e events.Event) Features {
	duration := e.EndTime.Sub(e.StartTime).Minutes()
	if duration <= 0 {
		duration = 60 // a duration-less event (no end_time) is assumed ~1hr for feature purposes
	}
	return Features{
		Type:               e.Type,
		ExpectedAttendance: e.ExpectedAttendance,
		Urgency:            e.Urgency,
		StartTime:          e.StartTime,
		DurationMinutes:    duration,
	}
}

func urgencyScore(u events.Urgency) float64 {
	switch u {
	case events.UrgencyCritical:
		return 3
	case events.UrgencyScheduledSpike:
		return 2
	case events.UrgencyHigh:
		return 1
	default:
		return 0
	}
}

// numFeatures is the length of vector()'s output: bias + one-hot(type) +
// attendance + urgency + hour + day-of-week + duration (5 trailing scalars).
func numFeatures() int {
	return 1 + len(eventTypes) + 5
}

// vector encodes Features as a numeric row for the model: [bias,
// one-hot(type)..., attendance/1000, urgency score, hour/24, weekday/7,
// duration/60]. Scaling each raw feature down to roughly [0,1] keeps
// gradient descent well-conditioned without a separate normalization pass
// baked into the model itself (Model.Train still standardizes on top of
// this, using the training set's own mean/stddev).
func (f Features) vector() []float64 {
	v := make([]float64, 0, numFeatures())
	v = append(v, 1) // bias term
	for _, t := range eventTypes {
		if f.Type == t {
			v = append(v, 1)
		} else {
			v = append(v, 0)
		}
	}
	v = append(v, float64(f.ExpectedAttendance)/1000)
	v = append(v, urgencyScore(f.Urgency))
	v = append(v, float64(f.StartTime.Hour())/24)
	v = append(v, float64(f.StartTime.Weekday())/7)
	v = append(v, f.DurationMinutes/60)
	return v
}
