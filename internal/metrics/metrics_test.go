package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// scrape returns the exposition-format body of a /metrics scrape.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func TestNewRegistriesAreIndependent(t *testing.T) {
	// Two Metrics in one process must not collide -- they would if this
	// package used prometheus.DefaultRegisterer, which panics on duplicate
	// registration. cmd/node and cmd/gateway are separate processes today,
	// but tests routinely build several in one.
	a := New("kv-node", "node-1")
	b := New("kv-node", "node-2")

	a.httpRequests.WithLabelValues("GET", "/healthz", "2xx").Inc()

	if !strings.Contains(scrape(t, a), `instance="node-1"`) {
		t.Error("registry a missing its own instance label")
	}
	if strings.Contains(scrape(t, b), `instance="node-1"`) {
		t.Error("registry b leaked registry a's metrics")
	}
}

func TestMiddlewareRecordsRouteNotRawPath(t *testing.T) {
	// The whole point of labelling by r.Pattern: a KV store gets one series
	// per route, not one per key. If this regresses, a busy cluster melts
	// its own Prometheus.
	m := New("kv-node", "node-1")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := m.Middleware(mux)

	for _, key := range []string{"alpha", "beta", "gamma"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/kv/"+key, nil))
	}

	body := scrape(t, m)
	if !strings.Contains(body, `route="GET /v1/kv/{key}"`) {
		t.Errorf("missing route-pattern label; body:\n%s", body)
	}
	for _, key := range []string{"alpha", "beta", "gamma"} {
		if strings.Contains(body, "/v1/kv/"+key) {
			t.Errorf("raw key %q leaked into a label -- unbounded cardinality", key)
		}
	}
}

func TestMiddlewareRecordsStatusClass(t *testing.T) {
	m := New("kv-node", "node-1")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	m.Middleware(mux).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))

	if body := scrape(t, m); !strings.Contains(body, `status="5xx"`) {
		t.Errorf("missing 5xx status label; body:\n%s", body)
	}
}

func TestMiddlewareRecordsImplicit200(t *testing.T) {
	// A handler that Writes without calling WriteHeader still produced a
	// 200; the recorder must not report 0.
	m := New("kv-node", "node-1")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hi"))
	})
	m.Middleware(mux).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))

	if body := scrape(t, m); !strings.Contains(body, `status="2xx"`) {
		t.Errorf("implicit 200 not recorded as 2xx; body:\n%s", body)
	}
}

func TestUnmatchedRouteLabelled(t *testing.T) {
	m := New("kv-node", "node-1")
	mux := http.NewServeMux()
	m.Middleware(mux).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/nope", nil))

	if body := scrape(t, m); !strings.Contains(body, `route="unmatched"`) {
		t.Errorf("404 should be labelled unmatched; body:\n%s", body)
	}
}

func TestRaftSourceIsPulledAtScrapeTime(t *testing.T) {
	m := New("kv-node", "node-1")
	term := uint64(1)
	leader := false
	m.RegisterRaftSource(func() RaftStats {
		return RaftStats{Term: term, IsLeader: leader, CommitIndex: 10, LastApplied: 7}
	})

	body := scrape(t, m)
	if !strings.Contains(body, "boilerpulse_raft_term") || !strings.Contains(body, "boilerpulse_raft_is_leader") {
		t.Fatalf("raft gauges missing; body:\n%s", body)
	}
	if !strings.Contains(body, "boilerpulse_raft_apply_lag") {
		t.Error("apply_lag gauge missing")
	}

	// Changing the underlying state must show up on the next scrape with no
	// explicit update call -- that's the point of GaugeFunc.
	term, leader = 42, true
	body = scrape(t, m)
	if !strings.Contains(body, `boilerpulse_raft_term{instance="node-1",service="kv-node"} 42`) {
		t.Errorf("term not re-read at scrape time; body:\n%s", body)
	}
	if !strings.Contains(body, `boilerpulse_raft_is_leader{instance="node-1",service="kv-node"} 1`) {
		t.Errorf("is_leader not re-read at scrape time; body:\n%s", body)
	}
}

func TestApplyLagNeverNegative(t *testing.T) {
	// lastApplied can briefly exceed commitIndex in a snapshot-restore path;
	// an unsigned subtraction there would underflow to a huge number.
	m := New("kv-node", "node-1")
	m.RegisterRaftSource(func() RaftStats {
		return RaftStats{CommitIndex: 5, LastApplied: 9}
	})
	if body := scrape(t, m); !strings.Contains(body, `boilerpulse_raft_apply_lag{instance="node-1",service="kv-node"} 0`) {
		t.Errorf("apply_lag should clamp to 0, not underflow; body:\n%s", body)
	}
}

func TestWorkloadModeExposesAllModes(t *testing.T) {
	m := New("gateway", "gateway")
	m.RegisterWorkloadSource(func() WorkloadStats {
		return WorkloadStats{Mode: "CRITICAL", RPS: 12.5}
	})

	body := scrape(t, m)
	for _, mode := range Modes {
		if !strings.Contains(body, `mode="`+mode+`"`) {
			t.Errorf("mode %q missing -- all modes should always be present", mode)
		}
	}
	if !strings.Contains(body, `boilerpulse_workload_mode{instance="gateway",mode="CRITICAL",service="gateway"} 1`) {
		t.Errorf("active mode should read 1; body:\n%s", body)
	}
	if !strings.Contains(body, `boilerpulse_workload_mode{instance="gateway",mode="NORMAL",service="gateway"} 0`) {
		t.Errorf("inactive mode should read 0; body:\n%s", body)
	}
}

func TestCacheAndStorageSources(t *testing.T) {
	m := New("kv-node", "node-1")
	m.RegisterCacheSource(func() CacheStats { return CacheStats{Hits: 3, Misses: 4, Evictions: 5} })
	m.RegisterStorageSource(func() StorageStats {
		return StorageStats{MemtableBytes: 100, MemtableEntries: 2, SSTables: 1}
	})

	body := scrape(t, m)
	for _, want := range []string{
		"boilerpulse_cache_hits_total",
		"boilerpulse_cache_misses_total",
		"boilerpulse_cache_evictions_total",
		"boilerpulse_storage_memtable_bytes",
		"boilerpulse_storage_memtable_entries",
		"boilerpulse_storage_sstables",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing metric %q", want)
		}
	}
}
