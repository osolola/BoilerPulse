package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// testOptions uses small timings so the test suite runs in well under a
// minute, while keeping enough margin over goroutine-scheduling jitter to
// avoid flakiness.
func testOptions() Options {
	return Options{
		MinElectionTimeout: 60 * time.Millisecond,
		MaxElectionTimeout: 120 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		TickInterval:       4 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeNetwork routes RPCs directly between in-process Nodes, and can
// simulate a node being disconnected (dropped/timed-out RPCs in both
// directions), for deterministic partition tests without real sockets.
type fakeNetwork struct {
	mu    sync.Mutex
	nodes map[string]*Node
	cut   map[string]bool
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{nodes: make(map[string]*Node), cut: make(map[string]bool)}
}

func (fn *fakeNetwork) register(n *Node) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.nodes[n.ID()] = n
}

func (fn *fakeNetwork) disconnect(id string) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.cut[id] = true
}

func (fn *fakeNetwork) reconnect(id string) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	delete(fn.cut, id)
}

func (fn *fakeNetwork) lookup(from, to string) (*Node, bool) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if fn.cut[from] || fn.cut[to] {
		return nil, false
	}
	n, ok := fn.nodes[to]
	return n, ok
}

type fakeTransport struct {
	net    *fakeNetwork
	selfID string
}

func (t *fakeTransport) SendRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	target, ok := t.net.lookup(t.selfID, peer)
	if !ok {
		return nil, errors.New("fake network: unreachable")
	}
	return target.HandleRequestVote(args), nil
}

func (t *fakeTransport) SendAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	target, ok := t.net.lookup(t.selfID, peer)
	if !ok {
		return nil, errors.New("fake network: unreachable")
	}
	return target.HandleAppendEntries(args), nil
}

// memStorage is a fast, non-durable Storage used only for algorithm tests.
// filestorage_test.go separately tests the real durable implementation.
type memStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	log      []LogEntry
}

func newMemStorage() *memStorage { return &memStorage{} }

func (s *memStorage) SaveTermAndVote(term uint64, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term, s.votedFor = term, votedFor
	return nil
}

func (s *memStorage) LoadTermAndVote() (uint64, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.votedFor, nil
}

func (s *memStorage) AppendEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return nil
}

func (s *memStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 {
		s.log = nil
		return nil
	}
	if index-1 < uint64(len(s.log)) {
		s.log = s.log[:index-1]
	}
	return nil
}

func (s *memStorage) LoadLog() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.log))
	copy(out, s.log)
	return out, nil
}

// testStateMachine records applied commands in order, for assertions.
type testStateMachine struct {
	mu      sync.Mutex
	applied [][]byte
}

func (sm *testStateMachine) Apply(cmd []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.applied = append(sm.applied, append([]byte(nil), cmd...))
	return nil
}

func (sm *testStateMachine) Applied() [][]byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make([][]byte, len(sm.applied))
	copy(out, sm.applied)
	return out
}

// testCluster wires up n in-process Nodes over a fakeNetwork, each with its
// own memStorage and testStateMachine.
type testCluster struct {
	net     *fakeNetwork
	nodes   []*Node
	sms     []*testStateMachine
	ids     []string
	mu      sync.Mutex
	stopped map[string]bool
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i+1)
	}

	net := newFakeNetwork()
	tc := &testCluster{net: net, ids: ids, stopped: make(map[string]bool)}

	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		sm := &testStateMachine{}
		node, err := NewNode(id, peers, newMemStorage(), &fakeTransport{net: net, selfID: id}, sm, testLogger(), testOptions())
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		net.register(node)
		tc.nodes = append(tc.nodes, node)
		tc.sms = append(tc.sms, sm)
	}

	return tc
}

func (tc *testCluster) startAll() {
	for _, n := range tc.nodes {
		n.Start()
	}
}

func (tc *testCluster) stopAll() {
	for _, n := range tc.nodes {
		tc.stopNode(n)
	}
}

// stopNode stops n and marks it as stopped, so leader() (and any test)
// stops considering it a live cluster member. Node.Stop halts n's
// background loops but deliberately leaves its last in-memory state
// (including State == Leader, if that's what it was) untouched -- Status()
// on a stopped node reflects "what it was doing when it died," which is
// exactly what a real crashed process's last-known state would look like.
// A test-only helper is the right place to filter that out, not Node itself.
func (tc *testCluster) stopNode(n *Node) {
	n.Stop()
	tc.mu.Lock()
	tc.stopped[n.ID()] = true
	tc.mu.Unlock()
}

// leader returns the live node with the highest term among those currently
// claiming leadership. A partitioned-but-still-running old leader can
// legitimately keep believing it's leader (correct Raft behavior — it just
// can never commit anything without a majority) at the same time a new
// leader is elected among the reachable majority with a higher term;
// picking the highest term is what correctly identifies the current one,
// since Raft guarantees at most one leader per term.
func (tc *testCluster) leader() *Node {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	var best *Node
	for _, n := range tc.nodes {
		if tc.stopped[n.ID()] || !n.IsLeader() {
			continue
		}
		if best == nil || n.Term() > best.Term() {
			best = n
		}
	}
	return best
}

// waitFor polls cond until it returns true or timeout elapses, failing the
// test on timeout. This keeps tests fast in the common case without being
// flaky under scheduler jitter.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for: %s", msg)
	}
}
