// Package gateway implements the API gateway (spec §24): it identifies the
// current Raft leader among a configured set of KV nodes, routes writes to
// it, distributes reads across all reachable nodes, aggregates real
// cluster status, and rate-limits clients. It is the only piece of
// BoilerPulse that knows about every node at once — each node itself only
// knows its own Raft status and a best-known leader hint.
package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"boilerpulse/internal/api"
	"boilerpulse/internal/cache"
	"boilerpulse/internal/prediction"
	"boilerpulse/internal/workload"
)

// Node is one configured KV node the gateway can route to.
type Node struct {
	ID   string
	Addr string // base URL, e.g. "http://localhost:8080"
	// AdminAddr is the node's chaos/failure-injection admin server (see
	// internal/admin), if any -- empty means the gateway's admin-proxy
	// routes return 503 for this node. Not the same as Addr: it's a
	// separate port, never exposed by the public KV API.
	AdminAddr string
}

// Options configures the gateway's behavior.
type Options struct {
	LeaderRefreshInterval time.Duration
	RequestTimeout        time.Duration
	RateLimit             RateLimitOptions
	Workload              workload.Thresholds
	CacheCapacity         int
	// CriticalHoldDuration is how long a CRITICAL-urgency event (spec §26)
	// forces the workload engine into CRITICAL mode, regardless of request
	// rate.
	CriticalHoldDuration time.Duration
	// PredictionTrainingSamples is how many synthetic samples to train the
	// traffic-prediction model on at startup (spec §67-C; see
	// internal/prediction). 0 disables prediction: POST /v1/predict then
	// returns 503.
	PredictionTrainingSamples int
	// AdminToken gates the gateway's /v1/admin/* proxy routes (see
	// admin_proxy.go) and is the same shared secret each node's own admin
	// server expects -- the gateway forwards it verbatim rather than
	// holding a separate credential. Empty disables the routes (503).
	AdminToken string
	// AllowedOrigin is the CORS Access-Control-Allow-Origin value. Empty
	// (DefaultOptions' default) means "*" -- fine for local dev, but a
	// public deployment should set this to the frontend's real origin
	// (see docs/deployment.md).
	AllowedOrigin string
}

// DefaultOptions returns production-sized settings.
func DefaultOptions() Options {
	return Options{
		LeaderRefreshInterval:     500 * time.Millisecond,
		RequestTimeout:            2 * time.Second,
		RateLimit:                 DefaultRateLimitOptions(),
		Workload:                  workload.DefaultThresholds(),
		CacheCapacity:             1000,
		CriticalHoldDuration:      5 * time.Minute,
		PredictionTrainingSamples: 2000,
		AllowedOrigin:             "*",
	}
}

// Gateway is an http.Handler that fronts a set of KV nodes.
type Gateway struct {
	nodes      []Node
	httpClient *http.Client
	logger     *slog.Logger
	opts       Options
	mux        *http.ServeMux
	limiter    *RateLimiter
	cache      *cache.LRU
	workload   *workload.Engine
	predictor  *prediction.Model

	mu         sync.RWMutex
	leaderID   string
	leaderAddr string
	nodeStatus map[string]nodeStatus // last known status per node ID, from the same poll used for leader discovery

	readCounter atomic.Uint64 // round-robin cursor for read distribution

	stopCh chan struct{}
	doneCh chan struct{}
}

type nodeStatus struct {
	reachable bool
	role      string
	term      uint64
}

// New builds a Gateway. Call Start to begin the background leader-discovery
// loop before serving traffic.
func New(nodes []Node, logger *slog.Logger, opts Options) *Gateway {
	if opts.AllowedOrigin == "" {
		opts.AllowedOrigin = "*"
	}
	predictor := prediction.NewModel()
	if opts.PredictionTrainingSamples > 0 {
		predictor.Train(prediction.GenerateSyntheticDataset(opts.PredictionTrainingSamples))
	}

	g := &Gateway{
		nodes:      nodes,
		httpClient: &http.Client{Timeout: opts.RequestTimeout},
		logger:     logger,
		opts:       opts,
		mux:        http.NewServeMux(),
		limiter:    NewRateLimiter(opts.RateLimit),
		cache:      cache.NewLRU(opts.CacheCapacity),
		workload:   workload.NewEngine(opts.Workload),
		predictor:  predictor,
		nodeStatus: make(map[string]nodeStatus, len(nodes)),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	g.routes()
	return g
}

func (g *Gateway) routes() {
	g.mux.HandleFunc("PUT /v1/kv/{key}", g.rateLimited(g.handleWrite))
	g.mux.HandleFunc("DELETE /v1/kv/{key}", g.rateLimited(g.handleWrite))
	g.mux.HandleFunc("GET /v1/kv/{key}", g.rateLimited(g.handleRead))
	g.mux.HandleFunc("POST /v1/events", g.rateLimited(g.handleWrite))
	g.mux.HandleFunc("GET /v1/events", g.rateLimited(g.handleRead))
	g.mux.HandleFunc("GET /v1/cluster", g.handleCluster)
	g.mux.HandleFunc("GET /v1/workload", g.handleWorkload)
	g.mux.HandleFunc("POST /v1/predict", g.rateLimited(g.handlePredict))
	g.mux.HandleFunc("GET /healthz", g.handleHealth)
	// Admin routes are rate-limited too (same per-client-IP limiter as
	// everything else) -- a stolen or brute-forced token shouldn't get
	// unlimited kill/fault attempts just because it's not /v1/kv/*.
	g.mux.HandleFunc("GET /v1/admin/status", g.rateLimited(g.authenticatedAdmin(g.handleAdminStatusAll)))
	g.mux.HandleFunc("GET /v1/admin/{nodeID}/status", g.rateLimited(g.authenticatedAdmin(g.handleAdminStatus)))
	g.mux.HandleFunc("POST /v1/admin/{nodeID}/kill", g.rateLimited(g.authenticatedAdmin(g.handleAdminKill)))
	g.mux.HandleFunc("POST /v1/admin/{nodeID}/fault", g.rateLimited(g.authenticatedAdmin(g.handleAdminFault)))
	g.mux.HandleFunc("POST /v1/admin/{nodeID}/restore", g.rateLimited(g.authenticatedAdmin(g.handleAdminRestore)))
}

// ServeHTTP applies permissive CORS (the frontend dashboard is a separate
// origin — a different port during local dev, e.g. :3000 vs the gateway's
// :8090 — and browsers block cross-origin fetches without these headers;
// curl-based testing never exercises this, browser testing caught it)
// before dispatching to the route mux.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", g.opts.AllowedOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	// Authorization is required here (unlike internal/api's CORS headers)
	// because the frontend's chaos controls (admin_proxy.go) send the
	// admin bearer token straight from the browser -- found by actually
	// driving the /cluster page in a real browser, which enforces CORS
	// preflight; curl-based testing never would have caught this.
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	g.mux.ServeHTTP(w, r)
}

// Start begins the background leader-discovery loop. It performs one
// synchronous refresh before returning, so the gateway has a best-effort
// view of the cluster as soon as Start returns.
func (g *Gateway) Start(ctx context.Context) {
	g.refresh(ctx)
	go g.run()
}

func (g *Gateway) Stop() {
	close(g.stopCh)
	<-g.doneCh
}

func (g *Gateway) run() {
	defer close(g.doneCh)
	ticker := time.NewTicker(g.opts.LeaderRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), g.opts.RequestTimeout)
			g.refresh(ctx)
			cancel()
		}
	}
}

func (g *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code api.ErrorCode, message string) {
	writeJSON(w, status, api.ErrorBody{Error: api.ErrorDetail{Code: code, Message: message}})
}
