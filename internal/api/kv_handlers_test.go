package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"boilerpulse/internal/storage"
)

func newTestServer() *Server {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(storage.NewMemStore(), logger, "test-node")
}

func doRequest(t *testing.T, s *Server, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestPutThenGet(t *testing.T) {
	s := newTestServer()

	putRec := doRequest(t, s, http.MethodPut, "/v1/kv/event:123",
		`{"value":{"title":"Purdue Basketball"},"consistency":"EVENTUAL","ttl_seconds":3600}`)
	if putRec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d; body=%s", putRec.Code, http.StatusNoContent, putRec.Body)
	}

	getRec := doRequest(t, s, http.MethodGet, "/v1/kv/event:123", "")
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body)
	}

	var resp getResponse
	if err := json.Unmarshal(getRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Consistency != "EVENTUAL" {
		t.Errorf("Consistency = %q, want %q", resp.Consistency, "EVENTUAL")
	}
	if string(resp.Value) != `{"title":"Purdue Basketball"}` {
		t.Errorf("Value = %s, want %s", resp.Value, `{"title":"Purdue Basketball"}`)
	}
}

func TestPutDefaultsConsistencyToEventual(t *testing.T) {
	s := newTestServer()
	doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)

	rec := doRequest(t, s, http.MethodGet, "/v1/kv/k", "")
	var resp getResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Consistency != "EVENTUAL" {
		t.Errorf("Consistency = %q, want %q", resp.Consistency, "EVENTUAL")
	}
}

func TestPutRejectsInvalidConsistency(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v","consistency":"WEAK"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, ErrInvalidRequest)
}

func TestPutRejectsMissingValue(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"consistency":"EVENTUAL"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, ErrInvalidRequest)
}

func TestPutRejectsMalformedJSON(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `not json`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, ErrInvalidRequest)
}

func TestPutRejectsNegativeTTL(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v","ttl_seconds":-1}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, ErrInvalidRequest)
}

func TestGetMissingKeyReturns404(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodGet, "/v1/kv/nope", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertErrorCode(t, rec, ErrKeyNotFound)
}

func TestDeleteThenGetReturns404(t *testing.T) {
	s := newTestServer()
	doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)

	delRec := doRequest(t, s, http.MethodDelete, "/v1/kv/k", "")
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", delRec.Code, http.StatusNoContent)
	}

	getRec := doRequest(t, s, http.MethodGet, "/v1/kv/k", "")
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want %d", getRec.Code, http.StatusNotFound)
	}
}

func TestDeleteMissingKeyReturns404(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodDelete, "/v1/kv/nope", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertErrorCode(t, rec, ErrKeyNotFound)
}

func TestHealthEndpoint(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodGet, "/healthz", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestCORSHeadersPresent guards against a real bug: browser-based clients
// (the frontend dashboard) got silently blocked by CORS with no headers
// set. curl-based tests never exercise CORS enforcement -- only an actual
// browser does -- so this locks the fix in at the HTTP-header level.
func TestCORSHeadersPresent(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodGet, "/healthz", "")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "*")
	}
}

// TestSetAllowedOriginRestrictsCORS covers the Milestone 11 hardening: a
// public deployment must be able to lock CORS down to its real frontend
// origin instead of the "*" dev default.
func TestSetAllowedOriginRestrictsCORS(t *testing.T) {
	s := newTestServer()
	s.SetAllowedOrigin("https://boilerpulse.example.com")
	rec := doRequest(t, s, http.MethodGet, "/healthz", "")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://boilerpulse.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "https://boilerpulse.example.com")
	}
}

// TestSetAllowedOriginEmptyRestoresWildcard guards against SetAllowedOrigin("")
// silently blocking every browser request instead of opting back into "*".
func TestSetAllowedOriginEmptyRestoresWildcard(t *testing.T) {
	s := newTestServer()
	s.SetAllowedOrigin("https://boilerpulse.example.com")
	s.SetAllowedOrigin("")
	rec := doRequest(t, s, http.MethodGet, "/healthz", "")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "*")
	}
}

func TestCORSPreflightRequestReturnsNoContent(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodOptions, "/v1/kv/k", "")

	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Error("Access-Control-Allow-Methods header missing on preflight response")
	}
}

func TestClusterEndpointReportsSingleNode(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodGet, "/v1/cluster", "")

	var resp clusterResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Mode != "SINGLE_NODE" {
		t.Errorf("Mode = %q, want %q", resp.Mode, "SINGLE_NODE")
	}
	if len(resp.Nodes) != 1 || resp.Nodes[0].ID != "test-node" {
		t.Errorf("Nodes = %+v, want single node with ID %q", resp.Nodes, "test-node")
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want ErrorCode) {
	t.Helper()
	var body ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if body.Error.Code != want {
		t.Errorf("error code = %q, want %q", body.Error.Code, want)
	}
}
