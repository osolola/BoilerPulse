// Package api implements the KV HTTP API (spec §14-§15) on top of a
// storage.Engine. It is the only externally reachable service in pass 1 —
// the gateway (cmd/gateway) that will front a multi-node cluster arrives in
// Milestone 4.
package api

import (
	"log/slog"
	"net/http"

	"boilerpulse/internal/storage"
)

// Server wires the storage engine to the KV HTTP API. It implements
// http.Handler so it can be used directly with http.ListenAndServe or
// wrapped in httptest for tests.
type Server struct {
	engine        storage.Engine
	logger        *slog.Logger
	nodeID        string
	mux           *http.ServeMux
	proposer      Proposer // optional; nil means writes go straight to engine
	allowedOrigin string
}

// NewServer builds a Server ready to handle requests. CORS defaults to "*"
// (any origin) — fine for local dev, where the frontend and backend run on
// different localhost ports and there's no untrusted traffic to worry
// about; call SetAllowedOrigin before a public deployment (see
// docs/deployment.md).
func NewServer(engine storage.Engine, logger *slog.Logger, nodeID string) *Server {
	s := &Server{
		engine:        engine,
		logger:        logger,
		nodeID:        nodeID,
		mux:           http.NewServeMux(),
		allowedOrigin: "*",
	}
	s.routes()
	return s
}

// SetProposer routes subsequent PUT/DELETE requests through Raft consensus
// instead of writing directly to the local storage engine. Called by
// cmd/node when Raft is enabled; left unset in tests and single-node
// configurations without Raft.
func (s *Server) SetProposer(p Proposer) {
	s.proposer = p
}

// SetAllowedOrigin restricts CORS to a single origin (e.g.
// "https://boilerpulse.example.com") instead of the "*" default. An empty
// origin is treated as "*" -- opting back into the permissive default
// rather than silently blocking every browser request.
func (s *Server) SetAllowedOrigin(origin string) {
	if origin == "" {
		origin = "*"
	}
	s.allowedOrigin = origin
}

func (s *Server) routes() {
	s.mux.HandleFunc("PUT /v1/kv/{key}", s.handlePut)
	s.mux.HandleFunc("GET /v1/kv/{key}", s.handleGet)
	s.mux.HandleFunc("DELETE /v1/kv/{key}", s.handleDelete)
	s.mux.HandleFunc("GET /v1/events", s.handleListEvents)
	s.mux.HandleFunc("POST /v1/events", s.handlePostEvent)
	s.mux.HandleFunc("GET /v1/cluster", s.handleCluster)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
}

// ServeHTTP satisfies http.Handler. It applies CORS (see SetAllowedOrigin)
// before dispatching, so a browser-based client (e.g. the frontend
// dashboard, pointed directly at a node instead of the gateway) can call
// it — mirrors internal/gateway.Gateway.ServeHTTP.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", s.allowedOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(w, r)
}
