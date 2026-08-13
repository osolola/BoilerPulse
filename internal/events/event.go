// Package events implements the normalized campus event model (spec §6-9):
// a common Event shape every source gets converted into, a pluggable
// EventSource interface, and the validate → normalize → classify →
// estimate-traffic pipeline. It has no dependency on the KV store — how
// events get stored is internal/api's job (POST/GET /v1/events).
package events

import "time"

// Type is one of the spec's campus event categories (§7).
type Type string

const (
	TypeAcademic       Type = "ACADEMIC"
	TypeAthletics      Type = "ATHLETICS"
	TypeStudentOrg     Type = "STUDENT_ORG"
	TypeCampusEvent    Type = "CAMPUS_EVENT"
	TypeDining         Type = "DINING"
	TypeTransportation Type = "TRANSPORTATION"
	TypeWeather        Type = "WEATHER"
	TypeEmergency      Type = "EMERGENCY"
	TypeSystem         Type = "SYSTEM"
)

func (t Type) valid() bool {
	switch t {
	case TypeAcademic, TypeAthletics, TypeStudentOrg, TypeCampusEvent,
		TypeDining, TypeTransportation, TypeWeather, TypeEmergency, TypeSystem:
		return true
	default:
		return false
	}
}

// Urgency reuses the same four workload classes the spec defines for the
// cluster as a whole (§5) — an event's urgency is what drives the
// workload-mode transitions described there (enforced starting Milestone 6).
type Urgency string

const (
	UrgencyNormal         Urgency = "NORMAL"
	UrgencyHigh           Urgency = "HIGH"
	UrgencyCritical       Urgency = "CRITICAL"
	UrgencyScheduledSpike Urgency = "SCHEDULED_SPIKE"
)

// Location is a campus place, per spec §6.
type Location struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
}

// Event is the normalized shape every source's raw data gets converted
// into, matching the spec's example (§6). Confidence is filled in by
// Normalize, not supplied by callers.
type Event struct {
	ID                        string    `json:"id"`
	Type                      Type      `json:"type"`
	Title                     string    `json:"title"`
	Description               string    `json:"description,omitempty"`
	Location                  Location  `json:"location"`
	StartTime                 time.Time `json:"start_time"`
	EndTime                   time.Time `json:"end_time,omitempty"`
	ExpectedAttendance        int       `json:"expected_attendance,omitempty"`
	ExpectedTrafficMultiplier float64   `json:"expected_traffic_multiplier,omitempty"`
	Urgency                   Urgency   `json:"urgency,omitempty"`
	Audience                  []string  `json:"audience,omitempty"`
	Source                    string    `json:"source,omitempty"`
	CreatedAt                 time.Time `json:"created_at,omitempty"`
	Confidence                float64   `json:"confidence,omitempty"`
}
