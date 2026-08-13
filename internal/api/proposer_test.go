package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"boilerpulse/internal/storage"
)

// fakeProposer simulates a Raft-backed Proposer without needing a real
// internal/raft.Node: Propose applies directly to the same engine the
// Server reads from, mimicking "commit and apply" without real consensus.
type fakeProposer struct {
	mu           sync.Mutex
	engine       storage.Engine
	isLeader     bool
	leaderID     string
	term         uint64
	proposeErr   error
	proposeCalls int
}

func (p *fakeProposer) Status() RaftStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := "FOLLOWER"
	if p.isLeader {
		state = "LEADER"
	}
	return RaftStatus{State: state, Term: p.term, LeaderID: p.leaderID, IsLeader: p.isLeader}
}

func (p *fakeProposer) Propose(ctx context.Context, cmd storage.Command) error {
	p.mu.Lock()
	p.proposeCalls++
	err := p.proposeErr
	p.mu.Unlock()

	if err != nil {
		return err
	}

	switch cmd.Op {
	case storage.CommandSet:
		return p.engine.Put(cmd.Key, cmd.Value, cmd.Consistency, time.Duration(cmd.TTLSeconds)*time.Second)
	case storage.CommandDelete:
		return p.engine.Delete(cmd.Key)
	default:
		return nil
	}
}

func (p *fakeProposer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.proposeCalls
}

func newServerWithProposer(engine storage.Engine, proposer *fakeProposer) *Server {
	s := newTestServerWithEngine(engine)
	s.SetProposer(proposer)
	return s
}

func TestPutWithProposerRoutesThroughProposeWhenLeader(t *testing.T) {
	engine := storage.NewMemStore()
	proposer := &fakeProposer{engine: engine, isLeader: true}
	s := newServerWithProposer(engine, proposer)

	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v","consistency":"STRONG"}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body)
	}
	if proposer.callCount() != 1 {
		t.Errorf("Propose called %d times, want 1", proposer.callCount())
	}

	getRec := doRequest(t, s, http.MethodGet, "/v1/kv/k", "")
	var resp getResponse
	json.Unmarshal(getRec.Body.Bytes(), &resp)
	if string(resp.Value) != `"v"` {
		t.Errorf("Value = %s, want %q", resp.Value, `"v"`)
	}
}

func TestPutWithProposerRejectsWhenNotLeader(t *testing.T) {
	engine := storage.NewMemStore()
	proposer := &fakeProposer{engine: engine, isLeader: false, leaderID: "node-2"}
	s := newServerWithProposer(engine, proposer)

	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, ErrLeaderUnavailable)

	var body ErrorBody
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Error.Message == "" {
		t.Error("expected a non-empty error message naming the current leader")
	}
	if proposer.callCount() != 0 {
		t.Errorf("Propose called %d times, want 0 (should reject before proposing)", proposer.callCount())
	}
}

func TestPutWithProposerReturns503OnProposeFailure(t *testing.T) {
	engine := storage.NewMemStore()
	proposer := &fakeProposer{engine: engine, isLeader: true, proposeErr: errors.New("lost leadership mid-flight")}
	s := newServerWithProposer(engine, proposer)

	rec := doRequest(t, s, http.MethodPut, "/v1/kv/k", `{"value":"v"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, rec, ErrLeaderUnavailable)
}

func TestDeleteWithProposerReturns404WithoutProposingForMissingKey(t *testing.T) {
	engine := storage.NewMemStore()
	proposer := &fakeProposer{engine: engine, isLeader: true}
	s := newServerWithProposer(engine, proposer)

	rec := doRequest(t, s, http.MethodDelete, "/v1/kv/nope", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if proposer.callCount() != 0 {
		t.Errorf("Propose called %d times, want 0 (missing key should short-circuit)", proposer.callCount())
	}
}

func TestDeleteWithProposerSucceedsForExistingKey(t *testing.T) {
	engine := storage.NewMemStore()
	if err := engine.Put("k", []byte("v"), storage.ConsistencyEventual, 0); err != nil {
		t.Fatalf("seeding engine: %v", err)
	}
	proposer := &fakeProposer{engine: engine, isLeader: true}
	s := newServerWithProposer(engine, proposer)

	rec := doRequest(t, s, http.MethodDelete, "/v1/kv/k", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if proposer.callCount() != 1 {
		t.Errorf("Propose called %d times, want 1", proposer.callCount())
	}
}

func TestClusterEndpointReportsRealRaftStatusWhenProposerSet(t *testing.T) {
	engine := storage.NewMemStore()
	proposer := &fakeProposer{engine: engine, isLeader: false, leaderID: "node-2", term: 7}
	s := newServerWithProposer(engine, proposer)

	rec := doRequest(t, s, http.MethodGet, "/v1/cluster", "")
	var resp clusterResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Mode != "RAFT" {
		t.Errorf("Mode = %q, want %q", resp.Mode, "RAFT")
	}
	if resp.Term != 7 {
		t.Errorf("Term = %d, want 7", resp.Term)
	}
	if len(resp.Nodes) != 2 {
		t.Fatalf("Nodes = %+v, want 2 entries (self + known leader)", resp.Nodes)
	}
	if resp.Nodes[0].Role != "FOLLOWER" {
		t.Errorf("self role = %q, want %q", resp.Nodes[0].Role, "FOLLOWER")
	}
	if resp.Nodes[1].ID != "node-2" || resp.Nodes[1].Role != "LEADER" {
		t.Errorf("leader entry = %+v, want ID=node-2 Role=LEADER", resp.Nodes[1])
	}
}

func newTestServerWithEngine(engine storage.Engine) *Server {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(engine, logger, "test-node")
}
