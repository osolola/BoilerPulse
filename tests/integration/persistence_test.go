package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"boilerpulse/internal/api"
	"boilerpulse/internal/storage/lsm"
)

// TestNodeSurvivesRestart drives the KV API over real HTTP, simulates a
// `kill -9` (no clean shutdown, no explicit flush), reopens the engine on
// the same data directory, and confirms the data is still there — the
// Milestone 2 goal from the spec: "kill process -> restart -> data survives".
func TestNodeSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	engine, err := lsm.Open(dir, logger, lsm.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ts := httptest.NewServer(api.NewServer(engine, logger, "it-node"))

	putBody := `{"value":{"title":"Purdue Basketball"},"consistency":"STRONG"}`
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/event:mackey", bytes.NewBufferString(putBody))
	if err != nil {
		t.Fatalf("building PUT request: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Simulate `kill -9` + restart: tear down the HTTP server and close only
	// the WAL file handle, with no chance for the engine to flush anything.
	ts.Close()
	if err := engine.CloseWALOnly(); err != nil {
		t.Fatalf("simulated-crash close: %v", err)
	}

	restarted, err := lsm.Open(dir, logger, lsm.DefaultOptions())
	if err != nil {
		t.Fatalf("Open after restart: %v", err)
	}
	defer restarted.Close()

	restartedTS := httptest.NewServer(api.NewServer(restarted, logger, "it-node"))
	defer restartedTS.Close()

	getResp, err := restartedTS.Client().Get(restartedTS.URL + "/v1/kv/event:mackey")
	if err != nil {
		t.Fatalf("GET after restart: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET after restart status = %d, want %d", getResp.StatusCode, http.StatusOK)
	}

	var body struct {
		Value       json.RawMessage `json:"value"`
		Consistency string          `json:"consistency"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if body.Consistency != "STRONG" {
		t.Errorf("Consistency after restart = %q, want %q", body.Consistency, "STRONG")
	}
	if string(body.Value) != `{"title":"Purdue Basketball"}` {
		t.Errorf("Value after restart = %s, want the pre-crash value", body.Value)
	}
}
