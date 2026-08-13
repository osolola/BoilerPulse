// Package integration exercises the KV API over a real HTTP connection
// (client -> net/http server -> storage engine), rather than calling
// handlers in-process the way internal/api's unit tests do.
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
	"boilerpulse/internal/storage"
)

func TestKVRoundTripOverHTTP(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := api.NewServer(storage.NewMemStore(), logger, "it-node")
	ts := httptest.NewServer(server)
	defer ts.Close()

	client := ts.Client()

	putBody := `{"value":{"title":"Purdue Basketball","location":"Mackey Arena"},"consistency":"EVENTUAL"}`
	putReq, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/event:mackey", bytes.NewBufferString(putBody))
	if err != nil {
		t.Fatalf("building PUT request: %v", err)
	}
	putResp, err := client.Do(putReq)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", putResp.StatusCode, http.StatusNoContent)
	}

	getResp, err := client.Get(ts.URL + "/v1/kv/event:mackey")
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getResp.StatusCode, http.StatusOK)
	}

	var body struct {
		Value       json.RawMessage `json:"value"`
		Consistency string          `json:"consistency"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding GET response: %v", err)
	}
	if body.Consistency != "EVENTUAL" {
		t.Errorf("Consistency = %q, want %q", body.Consistency, "EVENTUAL")
	}

	delReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/kv/event:mackey", nil)
	if err != nil {
		t.Fatalf("building DELETE request: %v", err)
	}
	delResp, err := client.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE request failed: %v", err)
	}
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", delResp.StatusCode, http.StatusNoContent)
	}

	finalGet, err := client.Get(ts.URL + "/v1/kv/event:mackey")
	if err != nil {
		t.Fatalf("final GET request failed: %v", err)
	}
	defer finalGet.Body.Close()
	if finalGet.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want %d", finalGet.StatusCode, http.StatusNotFound)
	}
}

func TestHealthAndClusterEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := api.NewServer(storage.NewMemStore(), logger, "it-node")
	ts := httptest.NewServer(server)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	clusterResp, err := ts.Client().Get(ts.URL + "/v1/cluster")
	if err != nil {
		t.Fatalf("GET /v1/cluster failed: %v", err)
	}
	defer clusterResp.Body.Close()
	if clusterResp.StatusCode != http.StatusOK {
		t.Errorf("/v1/cluster status = %d, want %d", clusterResp.StatusCode, http.StatusOK)
	}
}
