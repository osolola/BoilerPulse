// Package metrics exposes BoilerPulse's runtime state in Prometheus format.
//
// Two design choices worth knowing:
//
// Every metric is registered on a per-process *prometheus.Registry rather
// than the global default one, so a test can build a Metrics, scrape it, and
// throw it away without leaking collectors into other tests (the default
// registry is process-global and panics on duplicate registration).
//
// Everything except the HTTP counters is *pull-based*: gauges are backed by
// callbacks that run at scrape time (see the Register*Source methods), so a
// scrape always reports the live value and there is no background goroutine
// keeping a copy in sync. Sources are optional -- the gateway has no Raft or
// storage to report, and a single-node config has no Raft either.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "boilerpulse"

// RaftStats is the Raft state one node reports at scrape time. It mirrors
// the fields of raft.Status that are meaningful as time series (IDs and
// leader names are labels/log material, not gauges).
type RaftStats struct {
	Term         uint64
	IsLeader     bool
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
}

// StorageStats is the LSM engine state one node reports at scrape time.
type StorageStats struct {
	MemtableBytes   int
	MemtableEntries int
	SSTables        int
}

// CacheStats mirrors cache.Stats without importing it, keeping this package
// free of dependencies on the components it observes.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// WorkloadStats is the gateway's live workload view at scrape time.
type WorkloadStats struct {
	Mode string
	RPS  float64
}

// Metrics owns one process's registry and the collectors on it.
type Metrics struct {
	registry *prometheus.Registry
	labels   prometheus.Labels

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
}

// New builds a Metrics for one process. service is "kv-node" or "gateway";
// instance is the node ID (or "gateway"), so metrics from a 3-node cluster
// scraped into one Prometheus stay distinguishable.
func New(service, instance string) *Metrics {
	labels := prometheus.Labels{"service": service, "instance": instance}
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		labels:   labels,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "http",
			Name:        "requests_total",
			Help:        "Total HTTP requests handled, by route pattern and status.",
			ConstLabels: labels,
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   "http",
			Name:        "request_duration_seconds",
			Help:        "HTTP request latency, by route pattern.",
			ConstLabels: labels,
			// Tuned for this system's real measured range: single-digit
			// milliseconds normally, hundreds of ms when the WAL fsync
			// bottleneck bites (see docs/benchmarking.md).
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"method", "route"}),
	}

	reg.MustRegister(m.httpRequests, m.httpDuration)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the Prometheus exposition format for this process.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry, for tests that want to gather
// metrics directly instead of scraping over HTTP.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// statusRecorder captures the status code for the metrics middleware, since
// http.ResponseWriter does not expose what was written.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK // implicit 200 from a bare Write
	}
	return r.ResponseWriter.Write(b)
}

// Middleware records request count and latency around next.
//
// The route label is http.Request.Pattern -- the matched ServeMux pattern
// (e.g. "GET /v1/kv/{key}"), not the raw path. That distinction matters: the
// raw path would create one time series per key written, which for a KV
// store is unbounded cardinality and the classic way to melt a Prometheus.
// Pattern is only populated once the mux has routed, which is why it is read
// after next.ServeHTTP rather than before.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched" // 404s and preflights never match a pattern
		}
		status := rec.status
		if status == 0 {
			status = http.StatusOK // handler returned without writing anything
		}

		m.httpRequests.WithLabelValues(r.Method, route, statusText(status)).Inc()
		m.httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// statusText keeps the status label low-cardinality and readable.
func statusText(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

func (m *Metrics) gauge(subsystem, name, help string, fn func() float64) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   namespace,
		Subsystem:   subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: m.labels,
	}, fn))
}

// RegisterRaftSource wires Raft gauges to a live status callback. Call once,
// only when Raft is enabled.
func (m *Metrics) RegisterRaftSource(fn func() RaftStats) {
	m.gauge("raft", "term", "Current Raft term.", func() float64 { return float64(fn().Term) })
	m.gauge("raft", "is_leader", "1 if this node is the Raft leader, else 0.", func() float64 {
		if fn().IsLeader {
			return 1
		}
		return 0
	})
	m.gauge("raft", "commit_index", "Highest log index known to be committed.", func() float64 { return float64(fn().CommitIndex) })
	m.gauge("raft", "last_applied", "Highest log index applied to the state machine.", func() float64 { return float64(fn().LastApplied) })
	m.gauge("raft", "last_log_index", "Highest index in the local Raft log.", func() float64 { return float64(fn().LastLogIndex) })
	// Replication lag: how far the state machine trails the committed log.
	// A healthy node sits at 0; a sustained nonzero value means applying is
	// falling behind consensus.
	m.gauge("raft", "apply_lag", "commit_index minus last_applied.", func() float64 {
		s := fn()
		if s.CommitIndex < s.LastApplied {
			return 0
		}
		return float64(s.CommitIndex - s.LastApplied)
	})
}

// RegisterStorageSource wires LSM engine gauges to a live callback.
func (m *Metrics) RegisterStorageSource(fn func() StorageStats) {
	m.gauge("storage", "memtable_bytes", "Approximate size of the in-memory memtable.", func() float64 { return float64(fn().MemtableBytes) })
	m.gauge("storage", "memtable_entries", "Number of entries in the memtable.", func() float64 { return float64(fn().MemtableEntries) })
	m.gauge("storage", "sstables", "Number of on-disk SSTables.", func() float64 { return float64(fn().SSTables) })
}

// RegisterCacheSource wires gateway cache gauges to a live callback.
func (m *Metrics) RegisterCacheSource(fn func() CacheStats) {
	m.gauge("cache", "hits_total", "Cumulative cache hits.", func() float64 { return float64(fn().Hits) })
	m.gauge("cache", "misses_total", "Cumulative cache misses.", func() float64 { return float64(fn().Misses) })
	m.gauge("cache", "evictions_total", "Cumulative cache evictions.", func() float64 { return float64(fn().Evictions) })
}

// Modes is every workload mode that can be reported, so the mode gauge
// always exposes a full series set (0 for inactive modes) instead of a
// series appearing and disappearing as the mode changes -- the latter makes
// alerting and graphing awkward.
var Modes = []string{"NORMAL", "ELEVATED", "HIGH_TRAFFIC", "CRITICAL"}

// RegisterWorkloadSource wires workload gauges to a live callback.
func (m *Metrics) RegisterWorkloadSource(fn func() WorkloadStats) {
	m.gauge("workload", "rps", "Requests per second observed by the gateway.", func() float64 { return fn().RPS })

	for _, mode := range Modes {
		mode := mode
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "workload",
			Name:        "mode",
			Help:        "1 for the currently active workload mode, 0 for the others.",
			ConstLabels: mergeLabels(m.labels, prometheus.Labels{"mode": mode}),
		}, func() float64 {
			if fn().Mode == mode {
				return 1
			}
			return 0
		}))
	}
}

func mergeLabels(base, extra prometheus.Labels) prometheus.Labels {
	out := make(prometheus.Labels, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}
