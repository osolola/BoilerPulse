package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNode is a minimal stand-in for a real cmd/node HTTP server: it
// answers /v1/cluster with a configurable role/term and /v1/kv/{key} with
// simple in-memory storage, refusing writes unless role == "LEADER" (the
// same 503 LEADER_UNAVAILABLE shape internal/api actually returns).
type fakeNode struct {
	id     string
	server *httptest.Server

	mu        sync.Mutex
	role      string
	term      uint64
	kv        map[string]string
	putHits   int
	getHits   int
	eventHits int
}

func newFakeNode(id, role string, term uint64) *fakeNode {
	fn := &fakeNode{id: id, role: role, term: term, kv: make(map[string]string)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		fn.mu.Lock()
		resp := clusterPollResponse{
			Mode: "RAFT",
			Term: fn.term,
			Nodes: []struct {
				ID     string `json:"id"`
				Role   string `json:"role"`
				Status string `json:"status"`
			}{{ID: fn.id, Role: fn.role, Status: "HEALTHY"}},
		}
		fn.mu.Unlock()
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		fn.mu.Lock()
		fn.putHits++
		isLeader := fn.role == "LEADER"
		fn.mu.Unlock()
		if !isLeader {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "LEADER_UNAVAILABLE"}})
			return
		}
		body, _ := io.ReadAll(r.Body)
		fn.mu.Lock()
		fn.kv[r.PathValue("key")] = string(body)
		fn.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		fn.mu.Lock()
		fn.getHits++
		val, ok := fn.kv[r.PathValue("key")]
		fn.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write([]byte(val))
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		fn.mu.Lock()
		fn.eventHits++
		isLeader := fn.role == "LEADER"
		fn.mu.Unlock()
		if !isLeader {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "LEADER_UNAVAILABLE"}})
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	fn.server = httptest.NewServer(mux)
	return fn
}

func (fn *fakeNode) setRole(role string, term uint64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.role, fn.term = role, term
}

func (fn *fakeNode) node() Node { return Node{ID: fn.id, Addr: fn.server.URL} }

func (fn *fakeNode) close() { fn.server.Close() }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testOptionsNoRateLimit() Options {
	opts := DefaultOptions()
	opts.RateLimit = RateLimitOptions{} // disabled: RequestsPerSecond <= 0
	opts.PredictionTrainingSamples = 50 // small: most tests don't exercise prediction, keep New() fast
	return opts
}

// TestCORSHeadersPresent guards against a real bug found via actual browser
// testing (not curl, which never enforces CORS): the frontend dashboard's
// fetches to the gateway were silently blocked because no CORS headers
// were set.
func TestCORSHeadersPresent(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodGet, "/healthz", "")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "*")
	}
}

// TestProxiedResponseDoesNotDuplicateCORSHeaders guards against a real bug:
// a real node (internal/api.Server) sets its own Access-Control-Allow-*
// headers on every response, exactly like the gateway itself does. Before
// this fix, writeProxiedResponse copied every upstream header verbatim,
// which meant a proxied GET (/v1/kv/*, /v1/events) ended up with each
// Access-Control-* header sent TWICE -- once by the gateway's own
// ServeHTTP, once copied from the node's response. Browsers treat a
// duplicated Access-Control-Allow-Origin as invalid and fail the whole
// request (surfaced to users as "Load failed" in Safari, "Failed to
// fetch" elsewhere) even when both copies agree on the value -- curl
// doesn't enforce this, so it only shows up against a real browser.
func TestProxiedResponseDoesNotDuplicateCORSHeaders(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		// Mirrors internal/api.Server.ServeHTTP setting CORS on every
		// response, independent of whatever the gateway does.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Write([]byte(`{"value":"v"}`))
	})
	node := httptest.NewServer(mux)
	defer node.Close()

	gw := New([]Node{{ID: "node-1", Addr: node.URL}}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if got := rec.Header()["Access-Control-Allow-Origin"]; len(got) != 1 {
		t.Errorf("Access-Control-Allow-Origin header count = %d (%v), want exactly 1", len(got), got)
	}
	if got := rec.Header()["Access-Control-Allow-Methods"]; len(got) != 1 {
		t.Errorf("Access-Control-Allow-Methods header count = %d (%v), want exactly 1", len(got), got)
	}
}

// TestAllowedOriginRestrictsCORS covers the Milestone 11 hardening: a
// public deployment must be able to lock CORS down to its real frontend
// origin instead of the "*" dev default.
func TestAllowedOriginRestrictsCORS(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	opts := testOptionsNoRateLimit()
	opts.AllowedOrigin = "https://boilerpulse.example.com"
	gw := New([]Node{n1.node()}, testLogger(), opts)
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodGet, "/healthz", "")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://boilerpulse.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "https://boilerpulse.example.com")
	}
}

// TestEmptyAllowedOriginDefaultsToWildcard guards against a zero-value
// Options{} (e.g. built without DefaultOptions) silently blocking every
// browser request instead of falling back to "*".
func TestEmptyAllowedOriginDefaultsToWildcard(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	opts := testOptionsNoRateLimit()
	opts.AllowedOrigin = ""
	gw := New([]Node{n1.node()}, testLogger(), opts)
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodGet, "/healthz", "")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "*")
	}
}

// TestCORSAllowsAuthorizationHeader guards against a real bug found via
// actual browser testing (not curl) of the /cluster page's chaos controls:
// Access-Control-Allow-Headers only listed Content-Type, so the browser's
// CORS preflight rejected every admin-proxy request before it could even
// send the Authorization bearer token.
func TestCORSAllowsAuthorizationHeader(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodGet, "/healthz", "")
	got := rec.Header().Get("Access-Control-Allow-Headers")
	if !strings.Contains(got, "Authorization") {
		t.Errorf("Access-Control-Allow-Headers = %q, want it to include %q", got, "Authorization")
	}
}

func TestCORSPreflightRequestReturnsNoContent(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodOptions, "/v1/kv/k", "")
	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestHandleWriteRoutesToLeader(t *testing.T) {
	leader := newFakeNode("node-1", "LEADER", 1)
	follower := newFakeNode("node-2", "FOLLOWER", 1)
	defer leader.close()
	defer follower.close()

	gw := New([]Node{leader.node(), follower.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body)
	}

	leader.mu.Lock()
	putHits := leader.putHits
	leader.mu.Unlock()
	if putHits != 1 {
		t.Errorf("leader received %d PUTs, want 1", putHits)
	}

	follower.mu.Lock()
	followerHits := follower.putHits
	follower.mu.Unlock()
	if followerHits != 0 {
		t.Errorf("follower received %d PUTs, want 0", followerHits)
	}
}

func TestHandlePostEventsRoutesToLeader(t *testing.T) {
	leader := newFakeNode("node-1", "LEADER", 1)
	follower := newFakeNode("node-2", "FOLLOWER", 1)
	defer leader.close()
	defer follower.close()

	gw := New([]Node{leader.node(), follower.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodPost, "/v1/events", `{"type":"DINING","title":"Snack Bar","start_time":"2026-01-01T00:00:00Z"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/events status = %d, want %d; body=%s", rec.Code, http.StatusCreated, rec.Body)
	}

	leader.mu.Lock()
	hits := leader.eventHits
	leader.mu.Unlock()
	if hits != 1 {
		t.Errorf("leader received %d event POSTs, want 1", hits)
	}
}

func TestHandleWriteWithNoLeaderReturnsQuorumUnavailable(t *testing.T) {
	n1 := newFakeNode("node-1", "FOLLOWER", 1)
	n2 := newFakeNode("node-2", "FOLLOWER", 1)
	defer n1.close()
	defer n2.close()

	gw := New([]Node{n1.node(), n2.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleWriteRetriesAfterStaleLeader(t *testing.T) {
	oldLeader := newFakeNode("node-1", "LEADER", 1)
	newLeader := newFakeNode("node-2", "FOLLOWER", 1) // becomes leader mid-test
	defer oldLeader.close()
	defer newLeader.close()

	gw := New([]Node{oldLeader.node(), newLeader.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	// Simulate a failover that the gateway's background poller hasn't
	// picked up yet: old leader steps down, new leader takes over, but the
	// gateway's cache still points at the old one.
	oldLeader.setRole("FOLLOWER", 2)
	newLeader.setRole("LEADER", 2)

	rec := doRequest(t, gw, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d (gateway should refresh and retry)", rec.Code, http.StatusNoContent)
	}

	newLeader.mu.Lock()
	hits := newLeader.putHits
	newLeader.mu.Unlock()
	if hits != 1 {
		t.Errorf("new leader received %d PUTs, want 1", hits)
	}
}

// TestHandleWriteRetryCarriesOriginalBody guards against a real bug found
// during Milestone 6 development: proxyRequest used to read r.Body itself
// on every call, so the retried request after a stale-leader failure sent
// an EMPTY body to the new leader (the first attempt had already drained
// the stream). The other fakeNode-based tests didn't catch it because
// fakeNode's PUT handler doesn't validate body content -- this one checks
// the actual bytes the new leader received.
func TestHandleWriteRetryCarriesOriginalBody(t *testing.T) {
	oldLeader := newFakeNode("node-1", "LEADER", 1)
	newLeader := newFakeNode("node-2", "FOLLOWER", 1)
	defer oldLeader.close()
	defer newLeader.close()

	gw := New([]Node{oldLeader.node(), newLeader.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	oldLeader.setRole("FOLLOWER", 2)
	newLeader.setRole("LEADER", 2)

	const wantBody = `{"value":"real-content-not-empty"}`
	rec := doRequest(t, gw, http.MethodPut, "/v1/kv/k", wantBody)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	newLeader.mu.Lock()
	stored := newLeader.kv["k"]
	newLeader.mu.Unlock()
	if stored != wantBody {
		t.Errorf("new leader stored body = %q, want %q (retry must resend the original body, not an empty one)", stored, wantBody)
	}
}

func TestHandleReadDistributesAcrossNodes(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	n2 := newFakeNode("node-2", "FOLLOWER", 1)
	defer n1.close()
	defer n2.close()
	n1.kv["k"] = `"v"`
	n2.kv["k"] = `"v"`

	gw := New([]Node{n1.node(), n2.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	for i := 0; i < 10; i++ {
		rec := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET status = %d, want %d", rec.Code, http.StatusOK)
		}
	}

	n1.mu.Lock()
	n1Hits := n1.getHits
	n1.mu.Unlock()
	n2.mu.Lock()
	n2Hits := n2.getHits
	n2.mu.Unlock()

	if n1Hits == 0 || n2Hits == 0 {
		t.Errorf("reads not distributed: node-1=%d node-2=%d, want both > 0", n1Hits, n2Hits)
	}
	if n1Hits+n2Hits != 10 {
		t.Errorf("total GET hits = %d, want 10", n1Hits+n2Hits)
	}
}

func TestHandleReadFailsOverOnDeadNode(t *testing.T) {
	deadNode := newFakeNode("node-1", "LEADER", 1)
	live := newFakeNode("node-2", "FOLLOWER", 1)
	live.kv["k"] = `"v"`
	deadAddr := deadNode.node()
	deadNode.close() // simulate a node that's down

	gw := New([]Node{deadAddr, live.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()
	defer live.close()

	rec := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d (should fail over to the live node)", rec.Code, http.StatusOK)
	}
}

func TestHandleClusterReportsReachability(t *testing.T) {
	live := newFakeNode("node-1", "LEADER", 3)
	deadAddr := newFakeNode("node-2", "FOLLOWER", 3)
	target := deadAddr.node()
	deadAddr.close()
	defer live.close()

	gw := New([]Node{live.node(), target}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodGet, "/v1/cluster", "")
	var view clusterView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if view.LeaderID != "node-1" {
		t.Errorf("LeaderID = %q, want %q", view.LeaderID, "node-1")
	}
	if len(view.Nodes) != 2 {
		t.Fatalf("Nodes = %+v, want 2 entries", view.Nodes)
	}
	for _, n := range view.Nodes {
		wantReachable := n.ID == "node-1"
		if n.Reachable != wantReachable {
			t.Errorf("node %s Reachable = %v, want %v", n.ID, n.Reachable, wantReachable)
		}
	}
}

func TestHandleReadCachesEventualConsistencyResponses(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()
	n1.kv["k"] = `{"value":"v","consistency":"EVENTUAL"}`

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	first := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if first.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want %d", first.Code, http.StatusOK)
	}
	second := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if second.Code != http.StatusOK {
		t.Fatalf("second GET status = %d, want %d", second.Code, http.StatusOK)
	}
	if second.Header().Get("X-Cache") != "HIT" {
		t.Error("second GET was not served from cache (X-Cache != HIT)")
	}

	n1.mu.Lock()
	hits := n1.getHits
	n1.mu.Unlock()
	if hits != 1 {
		t.Errorf("node received %d GETs, want 1 (second should be a cache hit, not proxied)", hits)
	}
}

func TestHandleReadNeverCachesCriticalConsistency(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()
	n1.kv["k"] = `{"value":"evacuate","consistency":"CRITICAL"}`

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	second := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("CRITICAL-consistency response was served from cache, want never cached (spec §25)")
	}

	n1.mu.Lock()
	hits := n1.getHits
	n1.mu.Unlock()
	if hits != 2 {
		t.Errorf("node received %d GETs, want 2 (CRITICAL data must never be cached)", hits)
	}
}

func TestHandleWriteInvalidatesCache(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()
	n1.kv["k"] = `{"value":"old","consistency":"EVENTUAL"}`

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	doRequest(t, gw, http.MethodGet, "/v1/kv/k", "") // populates the cache

	doRequest(t, gw, http.MethodPut, "/v1/kv/k", `{"value":"new"}`)
	n1.mu.Lock()
	n1.kv["k"] = `{"value":"new","consistency":"EVENTUAL"}`
	n1.mu.Unlock()

	rec := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if rec.Header().Get("X-Cache") == "HIT" {
		t.Error("GET after a PUT to the same key was served from a stale cache entry, want invalidated")
	}
}

func TestWorkloadEndpointReportsEscalatedMode(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()
	n1.kv["k"] = `{"value":"v","consistency":"EVENTUAL"}`

	opts := testOptionsNoRateLimit()
	opts.Workload.ElevatedRPS = 1
	opts.Workload.HighTrafficRPS = 1000
	opts.Workload.CriticalRPS = 100000
	gw := New([]Node{n1.node()}, testLogger(), opts)
	gw.Start(context.Background())
	defer gw.Stop()

	for i := 0; i < 30; i++ {
		doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	}

	rec := doRequest(t, gw, http.MethodGet, "/v1/workload", "")
	var resp workloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Mode == "NORMAL" {
		t.Errorf("Mode = %q after a burst of requests, want an escalated mode", resp.Mode)
	}
	if resp.RPS <= 0 {
		t.Errorf("RPS = %v, want > 0", resp.RPS)
	}
}

func TestWorkloadEndpointTracksHotKeys(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()
	n1.kv["hot"] = `{"value":"v","consistency":"EVENTUAL"}`

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	for i := 0; i < 25; i++ {
		doRequest(t, gw, http.MethodGet, "/v1/kv/hot", "")
	}

	rec := doRequest(t, gw, http.MethodGet, "/v1/workload", "")
	var resp workloadResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	found := false
	for _, hk := range resp.HotKeys {
		if hk.Key == "hot" {
			found = true
		}
	}
	if !found {
		t.Errorf("HotKeys = %+v, want %q listed after 25 requests", resp.HotKeys, "hot")
	}
}

func TestHandlePredictReturnsPrediction(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	opts := testOptionsNoRateLimit()
	opts.PredictionTrainingSamples = 1500 // enough for a meaningfully trained model
	gw := New([]Node{n1.node()}, testLogger(), opts)
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodPost, "/v1/predict",
		`{"type":"ATHLETICS","title":"Purdue Basketball","expected_attendance":14000,"start_time":"2026-11-20T19:00:00Z"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body)
	}

	var out struct {
		PredictedRPS     float64 `json:"predicted_rps"`
		Confidence       float64 `json:"confidence"`
		RecommendedNodes int     `json:"recommended_nodes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.PredictedRPS <= 0 {
		t.Errorf("PredictedRPS = %v, want > 0 for a large athletics event", out.PredictedRPS)
	}
	if out.RecommendedNodes < 1 {
		t.Errorf("RecommendedNodes = %d, want >= 1", out.RecommendedNodes)
	}
}

func TestHandlePredictRejectsMissingStartTime(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()

	gw := New([]Node{n1.node()}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	rec := doRequest(t, gw, http.MethodPost, "/v1/predict", `{"type":"ATHLETICS","title":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPostCriticalEventSignalsCriticalMode(t *testing.T) {
	// A minimal fake node that behaves like the real internal/api: it
	// responds to POST /v1/events with the *normalized* event, including a
	// server-computed urgency, regardless of what the client sent -- the
	// gateway is supposed to read urgency from this response, not guess
	// from the request.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(clusterPollResponse{
			Mode: "RAFT", Term: 1,
			Nodes: []struct {
				ID     string `json:"id"`
				Role   string `json:"role"`
				Status string `json:"status"`
			}{{ID: "node-1", Role: "LEADER", Status: "HEALTHY"}},
		})
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"id": "evt_1", "urgency": "CRITICAL"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	gw := New([]Node{{ID: "node-1", Addr: server.URL}}, testLogger(), testOptionsNoRateLimit())
	gw.Start(context.Background())
	defer gw.Stop()

	if mode := gw.workload.Mode(); mode != "NORMAL" {
		t.Fatalf("initial mode = %q, want NORMAL", mode)
	}

	rec := doRequest(t, gw, http.MethodPost, "/v1/events", `{"type":"EMERGENCY","title":"Evacuate","start_time":"2026-01-01T00:00:00Z"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, want %d", rec.Code, http.StatusCreated)
	}

	if mode := gw.workload.Mode(); mode != "CRITICAL" {
		t.Errorf("Mode() after a CRITICAL event = %q, want %q", mode, "CRITICAL")
	}
}

func TestRateLimiterAllowsBurstThenBlocks(t *testing.T) {
	rl := NewRateLimiter(RateLimitOptions{RequestsPerSecond: 1, Burst: 3})

	for i := 0; i < 3; i++ {
		if !rl.Allow("client-a") {
			t.Fatalf("request %d denied, want allowed (within burst)", i)
		}
	}
	if rl.Allow("client-a") {
		t.Error("4th immediate request allowed, want denied (burst exhausted)")
	}
	if !rl.Allow("client-b") {
		t.Error("different client denied, want allowed (separate bucket)")
	}
}

func TestRateLimiterRefillsOverTime(t *testing.T) {
	rl := NewRateLimiter(RateLimitOptions{RequestsPerSecond: 100, Burst: 1})

	if !rl.Allow("k") {
		t.Fatal("first request denied, want allowed")
	}
	if rl.Allow("k") {
		t.Fatal("second immediate request allowed, want denied")
	}

	time.Sleep(20 * time.Millisecond) // >= 1/100s, enough for >=1 token to refill
	if !rl.Allow("k") {
		t.Error("request after refill window denied, want allowed")
	}
}

func TestRateLimitedMiddlewareReturns429(t *testing.T) {
	n1 := newFakeNode("node-1", "LEADER", 1)
	defer n1.close()
	n1.kv["k"] = `"v"`

	opts := DefaultOptions()
	opts.RateLimit = RateLimitOptions{RequestsPerSecond: 0.001, Burst: 1}
	gw := New([]Node{n1.node()}, testLogger(), opts)
	gw.Start(context.Background())
	defer gw.Stop()

	first := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if first.Code != http.StatusOK {
		t.Fatalf("first GET status = %d, want %d", first.Code, http.StatusOK)
	}
	second := doRequest(t, gw, http.MethodGet, "/v1/kv/k", "")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second GET status = %d, want %d", second.Code, http.StatusTooManyRequests)
	}
}

func doRequest(t *testing.T, gw *Gateway, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	return rec
}
