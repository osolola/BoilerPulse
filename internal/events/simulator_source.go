package events

import (
	"context"
	"math/rand"
	"time"
)

// simulatorCatalog is a small set of representative campus event templates.
// SimulatorSource picks from these rather than scraping anything — per spec
// §40, the simulator is the first and most trustworthy source, and the
// system must work with only it configured.
var simulatorCatalog = []Event{
	{
		Type: TypeAthletics, Title: "Purdue Basketball",
		Location:           Location{Name: "Mackey Arena", Latitude: 40.434, Longitude: -86.918},
		ExpectedAttendance: 14000, Audience: []string{"CAMPUS"},
	},
	{
		Type: TypeDining, Title: "Dining Court Extended Hours",
		Location:           Location{Name: "Wiley Dining Court", Latitude: 40.425, Longitude: -86.916},
		ExpectedAttendance: 300, Audience: []string{"CAMPUS"},
	},
	{
		Type: TypeStudentOrg, Title: "Student Org Club Fair",
		Location:           Location{Name: "Purdue Memorial Union", Latitude: 40.425, Longitude: -86.910},
		ExpectedAttendance: 400, Audience: []string{"CAMPUS"},
	},
	{
		Type: TypeAcademic, Title: "Midterm Week Notice",
		Location: Location{Name: "Campus-wide"}, Audience: []string{"CAMPUS"},
	},
	{
		Type: TypeTransportation, Title: "Bus Route Delay",
		Location: Location{Name: "Transit Center", Latitude: 40.428, Longitude: -86.914},
		Audience: []string{"CAMPUS"},
	},
	{
		Type: TypeWeather, Title: "Severe Weather Advisory",
		Location: Location{Name: "Campus-wide"}, Audience: []string{"CAMPUS"},
	},
	{
		Type: TypeEmergency, Title: "Emergency Alert Drill",
		Location: Location{Name: "Campus-wide"}, Audience: []string{"CAMPUS"},
	},
}

// SimulatorSource generates synthetic campus events from a small template
// catalog. It's the default and only EventSource wired up by cmd/ingest —
// public/legitimate external sources are optional adapters added later
// (spec §40), never a hard dependency.
type SimulatorSource struct{}

func NewSimulatorSource() *SimulatorSource { return &SimulatorSource{} }

func (s *SimulatorSource) Name() string { return "SIMULATOR" }

// FetchEvents returns 1-3 randomly generated events per call.
func (s *SimulatorSource) FetchEvents(ctx context.Context) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := time.Now()
	n := 1 + rand.Intn(3)
	out := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		e := simulatorCatalog[rand.Intn(len(simulatorCatalog))]
		e.StartTime = now.Add(time.Duration(rand.Intn(72)) * time.Hour)
		e.EndTime = e.StartTime.Add(2 * time.Hour)
		e.Source = s.Name()
		out = append(out, e)
	}
	return out, nil
}
