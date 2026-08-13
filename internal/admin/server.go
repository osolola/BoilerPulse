// Package admin exposes per-node chaos/failure-injection controls (spec
// §23, §34): kill this process, inject network faults (latency, packet
// drop, partition), clear them, and report status. Mounted by cmd/node on
// a separate port from the public KV API and gateway, and gated by a
// shared-secret bearer token — see docs/failure-testing.md for exactly
// what that does and does not protect against, and why full auth hardening
// is still Milestone 11's job before any of this goes near a public URL.
package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"boilerpulse/internal/raft"
	"boilerpulse/internal/raft/rpc"
)

// Server is the admin HTTP handler for one node.
type Server struct {
	node   *raft.Node
	faults *rpc.Faults
	logger *slog.Logger
	token  string
	mux    *http.ServeMux
	exit   func(code int) // os.Exit by default; overridden in tests so they don't kill the test binary
}

// NewServer builds a Server. An empty token disables every endpoint
// (rather than falling back to "no auth required") — see authenticated.
func NewServer(node *raft.Node, faults *rpc.Faults, logger *slog.Logger, token string) *Server {
	s := &Server{node: node, faults: faults, logger: logger, token: token, mux: http.NewServeMux(), exit: os.Exit}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /kill", s.authenticated(s.handleKill))
	s.mux.HandleFunc("POST /fault", s.authenticated(s.handleFault))
	s.mux.HandleFunc("POST /restore", s.authenticated(s.handleRestore))
	s.mux.HandleFunc("GET /status", s.authenticated(s.handleStatus))
}

// ServeHTTP satisfies http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// authenticated requires a "Bearer <token>" Authorization header matching
// the configured token. An empty configured token disables the endpoint
// entirely (503) rather than silently allowing unauthenticated access —
// admin endpoints must be opted into deliberately, never on by default.
func (s *Server) authenticated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token == "" {
			writeError(w, http.StatusServiceUnavailable, "admin endpoints are disabled: no admin token configured")
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+s.token {
			writeError(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
		next(w, r)
	}
}

// handleKill simulates a crash: it responds first, then exits the process
// ungracefully (no flush, no clean shutdown) shortly after — deliberately
// the same "no chance to clean up" scenario every crash-recovery test
// elsewhere in this project (WAL, Raft log, cluster failover) has already
// been validated against.
func (s *Server) handleKill(w http.ResponseWriter, r *http.Request) {
	s.logger.Warn("admin: kill requested -- exiting ungracefully to simulate a crash")
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "killing"})
	go func() {
		time.Sleep(100 * time.Millisecond) // let the response actually flush before we exit
		s.exit(1)
	}()
}

type faultRequest struct {
	Partitioned *bool    `json:"partitioned,omitempty"`
	LatencyMS   *int64   `json:"latency_ms,omitempty"`
	DropRate    *float64 `json:"drop_rate,omitempty"`
}

// handleFault sets whichever fields are present in the request, leaving
// others at their current value — so e.g. setting latency doesn't
// implicitly clear an existing partition.
func (s *Server) handleFault(w http.ResponseWriter, r *http.Request) {
	var req faultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if req.Partitioned != nil {
		s.faults.SetPartitioned(*req.Partitioned)
	}
	if req.LatencyMS != nil {
		s.faults.SetLatency(time.Duration(*req.LatencyMS) * time.Millisecond)
	}
	if req.DropRate != nil {
		s.faults.SetDropRate(*req.DropRate)
	}

	s.logger.Warn("admin: fault injected",
		"partitioned", req.Partitioned, "latency_ms", req.LatencyMS, "drop_rate", req.DropRate)
	writeJSON(w, http.StatusOK, s.statusResponse())
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	s.faults.Reset()
	s.logger.Info("admin: faults cleared")
	writeJSON(w, http.StatusOK, s.statusResponse())
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.statusResponse())
}

type statusView struct {
	Faults rpc.Status `json:"faults"`
	Raft   raftView   `json:"raft"`
}

type raftView struct {
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
}

func (s *Server) statusResponse() statusView {
	if s.node == nil {
		// Raft isn't enabled on this node (no peers configured) -- kill and
		// fault-injection endpoints still work, there's just no Raft state
		// to report.
		return statusView{Faults: s.faults.Status(), Raft: raftView{State: "DISABLED"}}
	}
	raftStatus := s.node.Status()
	return statusView{
		Faults: s.faults.Status(),
		Raft:   raftView{State: raftStatus.State.String(), Term: raftStatus.Term, LeaderID: raftStatus.LeaderID},
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
