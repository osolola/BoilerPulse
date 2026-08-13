package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"boilerpulse/internal/events"
)

func TestPostEventNormalizesAndStores(t *testing.T) {
	s := newTestServer()

	body := `{"type":"ATHLETICS","title":"Purdue Basketball","start_time":"` +
		time.Now().Add(time.Hour).Format(time.RFC3339) + `","expected_attendance":14000}`
	rec := doRequest(t, s, http.MethodPost, "/v1/events", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body)
	}

	var created events.Event
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if created.ID == "" {
		t.Error("response event has no ID assigned")
	}
	if created.Urgency != events.UrgencyHigh {
		t.Errorf("Urgency = %q, want %q (14000 attendance should classify HIGH)", created.Urgency, events.UrgencyHigh)
	}

	getRec := doRequest(t, s, http.MethodGet, "/v1/kv/event:"+created.ID, "")
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET stored event status = %d, want %d", getRec.Code, http.StatusOK)
	}
}

func TestPostEventRejectsInvalidEvent(t *testing.T) {
	s := newTestServer()

	rec := doRequest(t, s, http.MethodPost, "/v1/events", `{"type":"ATHLETICS"}`) // no title, no start_time
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, ErrInvalidRequest)
}

func TestPostEventCriticalGetsCriticalConsistency(t *testing.T) {
	s := newTestServer()

	body := `{"type":"EMERGENCY","title":"Evacuation Notice","start_time":"` +
		time.Now().Format(time.RFC3339) + `"}`
	rec := doRequest(t, s, http.MethodPost, "/v1/events", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusCreated)
	}

	var created events.Event
	json.Unmarshal(rec.Body.Bytes(), &created)

	getRec := doRequest(t, s, http.MethodGet, "/v1/kv/event:"+created.ID, "")
	var stored getResponse
	json.Unmarshal(getRec.Body.Bytes(), &stored)
	if stored.Consistency != "CRITICAL" {
		t.Errorf("stored Consistency = %q, want %q", stored.Consistency, "CRITICAL")
	}
}

func TestListEventsReturnsSortedByStartTime(t *testing.T) {
	s := newTestServer()

	later := time.Now().Add(48 * time.Hour).Format(time.RFC3339)
	sooner := time.Now().Add(time.Hour).Format(time.RFC3339)

	doRequest(t, s, http.MethodPost, "/v1/events", `{"type":"DINING","title":"Later Event","start_time":"`+later+`"}`)
	doRequest(t, s, http.MethodPost, "/v1/events", `{"type":"DINING","title":"Sooner Event","start_time":"`+sooner+`"}`)

	rec := doRequest(t, s, http.MethodGet, "/v1/events", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/events status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Events []events.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Events) != 2 {
		t.Fatalf("got %d events, want 2", len(resp.Events))
	}
	if resp.Events[0].Title != "Sooner Event" || resp.Events[1].Title != "Later Event" {
		t.Errorf("events not sorted by start_time: got %q then %q", resp.Events[0].Title, resp.Events[1].Title)
	}
}

func TestListEventsExcludesNonEventKeys(t *testing.T) {
	s := newTestServer()

	doRequest(t, s, http.MethodPut, "/v1/kv/not-an-event", `{"value":"x"}`)
	doRequest(t, s, http.MethodPost, "/v1/events", `{"type":"DINING","title":"Real Event","start_time":"`+time.Now().Format(time.RFC3339)+`"}`)

	rec := doRequest(t, s, http.MethodGet, "/v1/events", "")
	var resp struct {
		Events []events.Event `json:"events"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Events) != 1 {
		t.Fatalf("got %d events, want 1 (non-event key should be excluded)", len(resp.Events))
	}
}

func TestListEventsEmptyReturnsEmptyArray(t *testing.T) {
	s := newTestServer()

	rec := doRequest(t, s, http.MethodGet, "/v1/events", "")
	var resp struct {
		Events []events.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Events) != 0 {
		t.Errorf("got %d events, want 0", len(resp.Events))
	}
}
