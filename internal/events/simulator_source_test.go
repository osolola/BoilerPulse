package events

import (
	"context"
	"testing"
	"time"
)

func TestSimulatorSourceProducesValidEvents(t *testing.T) {
	s := NewSimulatorSource()
	if s.Name() != "SIMULATOR" {
		t.Errorf("Name() = %q, want %q", s.Name(), "SIMULATOR")
	}

	for i := 0; i < 20; i++ { // run several times since generation is randomized
		got, err := s.FetchEvents(context.Background())
		if err != nil {
			t.Fatalf("FetchEvents: %v", err)
		}
		if len(got) < 1 || len(got) > 3 {
			t.Fatalf("FetchEvents returned %d events, want 1-3", len(got))
		}
		for _, e := range got {
			if _, err := Normalize(e); err != nil {
				t.Errorf("simulator produced an event that fails Normalize: %v (event=%+v)", err, e)
			}
			if e.Source != "SIMULATOR" {
				t.Errorf("Source = %q, want %q", e.Source, "SIMULATOR")
			}
			if e.EndTime.Before(e.StartTime) {
				t.Errorf("EndTime %v is before StartTime %v", e.EndTime, e.StartTime)
			}
		}
	}
}

func TestSimulatorSourceRespectsCancelledContext(t *testing.T) {
	s := NewSimulatorSource()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.FetchEvents(ctx); err == nil {
		t.Error("FetchEvents with a cancelled context returned nil error, want an error")
	}
}

type stubSource struct {
	name   string
	events []Event
	err    error
}

func (s *stubSource) Name() string { return s.name }
func (s *stubSource) FetchEvents(ctx context.Context) ([]Event, error) {
	return s.events, s.err
}

func TestFetchAndNormalizeAssignsSourceWhenMissing(t *testing.T) {
	src := &stubSource{name: "TEST_SOURCE", events: []Event{
		{Type: TypeDining, Title: "Snack Bar Open", StartTime: time.Now().Add(time.Hour)},
	}}

	got, err := FetchAndNormalize(context.Background(), src)
	if err != nil {
		t.Fatalf("FetchAndNormalize: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].Source != "TEST_SOURCE" {
		t.Errorf("Source = %q, want %q", got[0].Source, "TEST_SOURCE")
	}
}

func TestFetchAndNormalizeSkipsInvalidEventsWithoutFailingBatch(t *testing.T) {
	src := &stubSource{name: "TEST_SOURCE", events: []Event{
		{Type: TypeDining, Title: "Valid Event", StartTime: time.Now().Add(time.Hour)},
		{Type: TypeDining, Title: "", StartTime: time.Now().Add(time.Hour)}, // invalid: no title
	}}

	got, err := FetchAndNormalize(context.Background(), src)
	if err != nil {
		t.Fatalf("FetchAndNormalize: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1 (invalid event should be skipped, not fail the batch)", len(got))
	}
	if got[0].Title != "Valid Event" {
		t.Errorf("Title = %q, want %q", got[0].Title, "Valid Event")
	}
}

func TestFetchAndNormalizePropagatesSourceError(t *testing.T) {
	src := &stubSource{name: "TEST_SOURCE", err: context.DeadlineExceeded}

	if _, err := FetchAndNormalize(context.Background(), src); err == nil {
		t.Error("FetchAndNormalize with a failing source returned nil error, want an error")
	}
}
