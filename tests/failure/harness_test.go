// Package failure covers spec §48's four failure scenarios end to end,
// against real components (real lsm.Engine on disk, real raft.Node, real
// gRPC transport, real KV HTTP API) -- the same pieces cmd/node assembles,
// wired here directly so a test can kill and restart a specific node
// precisely and assert on the cluster's actual behavior, not a mock of it.
package failure

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"context"

	"google.golang.org/grpc"

	"boilerpulse/internal/api"
	"boilerpulse/internal/raft"
	raftrpc "boilerpulse/internal/raft/rpc"
	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/lsm"
	"boilerpulse/pkg/protocol/raftpb"
)

func testRaftOptions() raft.Options {
	return raft.Options{
		MinElectionTimeout: 80 * time.Millisecond,
		MaxElectionTimeout: 160 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		TickInterval:       5 * time.Millisecond,
		RPCTimeout:         200 * time.Millisecond,
	}
}

type memberProposer struct{ node *raft.Node }

func (p *memberProposer) Propose(ctx context.Context, cmd storage.Command) error {
	data, err := storage.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	return p.node.Propose(ctx, data)
}

func (p *memberProposer) Status() api.RaftStatus {
	s := p.node.Status()
	return api.RaftStatus{State: s.State.String(), Term: s.Term, LeaderID: s.LeaderID, IsLeader: s.State == raft.Leader}
}

// member is one cluster node built from real, on-disk components -- unlike
// tests/integration's clusterMember (which never restarts), member.dataDir
// and member.raftDir persist for the lifetime of the test so kill()
// followed by start() is a real restart with real recovered state, not a
// fresh node.
type member struct {
	id      string
	peers   []string
	addr    string // raft grpc addr; fixed across restarts so peers can always find it again
	dataDir string
	raftDir string

	logger *slog.Logger
	addrs  map[string]string

	mu         sync.Mutex
	node       *raft.Node
	engine     *lsm.Engine
	raftStore  *raft.FileStorage
	grpcServer *grpc.Server
	httpServer *httptest.Server
	faults     *raftrpc.Faults
	running    bool
}

func (m *member) start(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		t.Fatalf("member %s already running", m.id)
	}

	engine, err := lsm.Open(m.dataDir, m.logger, lsm.DefaultOptions())
	if err != nil {
		t.Fatalf("lsm.Open(%s): %v", m.id, err)
	}
	raftStore, err := raft.OpenFileStorage(m.raftDir)
	if err != nil {
		engine.Close()
		t.Fatalf("OpenFileStorage(%s): %v", m.id, err)
	}

	faults := raftrpc.NewFaults()
	transport := raftrpc.NewTransport(m.addrs, faults)
	sm := storage.NewRaftStateMachine(engine)
	node, err := raft.NewNode(m.id, m.peers, raftStore, transport, sm, m.logger, testRaftOptions())
	if err != nil {
		raftStore.Close()
		engine.Close()
		t.Fatalf("NewNode(%s): %v", m.id, err)
	}

	lis, err := net.Listen("tcp", m.addr)
	if err != nil {
		raftStore.Close()
		engine.Close()
		t.Fatalf("net.Listen(%s, %s): %v", m.id, m.addr, err)
	}
	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, raftrpc.NewServer(node, faults))
	go grpcServer.Serve(lis)

	node.Start()

	apiServer := api.NewServer(engine, m.logger, m.id)
	apiServer.SetProposer(&memberProposer{node: node})
	httpServer := httptest.NewServer(apiServer)

	m.node = node
	m.engine = engine
	m.raftStore = raftStore
	m.grpcServer = grpcServer
	m.httpServer = httpServer
	m.faults = faults
	m.running = true
}

// kill abruptly tears the member down: no graceful Raft step-down, the
// same "no chance to clean up" scenario the WAL/Raft-log crash-recovery
// tests elsewhere in this project already validate against (spec §48
// calls this "kill this node"). Data on disk survives, so a later start()
// is a real restart, not a fresh node.
func (m *member) kill(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running {
		return
	}
	m.httpServer.Close()
	m.node.Stop()
	m.grpcServer.Stop()
	m.raftStore.Close()
	m.engine.Close()
	m.running = false
}

func (m *member) isRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

func (m *member) isLeader() bool {
	m.mu.Lock()
	node, running := m.node, m.running
	m.mu.Unlock()
	return running && node != nil && node.IsLeader()
}

func (m *member) term() uint64 {
	m.mu.Lock()
	node, running := m.node, m.running
	m.mu.Unlock()
	if !running || node == nil {
		return 0
	}
	return node.Term()
}

func (m *member) url() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.httpServer.URL
}

// newFailureCluster wires n real nodes, each on its own persistent temp
// directory pair (so a kill()/start() cycle within the test is a real
// restart), and blocks until t.Cleanup so every member is torn down at
// test end regardless of what the test itself already killed.
func newFailureCluster(t *testing.T, n int) []*member {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i+1)
	}

	// Reserve an address per node up front (bind briefly, then release) so
	// every peer's address map is stable for the cluster's lifetime, even
	// across a member's restart.
	addrs := make(map[string]string, n)
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserving addr for %s: %v", id, err)
		}
		addrs[id] = lis.Addr().String()
		lis.Close()
	}

	members := make([]*member, n)
	for i, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		m := &member{
			id:      id,
			peers:   peers,
			addr:    addrs[id],
			dataDir: t.TempDir(),
			raftDir: t.TempDir(),
			logger:  logger,
			addrs:   addrs,
		}
		m.start(t)
		members[i] = m
		t.Cleanup(func() { m.kill(t) })
	}
	return members
}

func waitForLeader(members []*member, timeout time.Duration) *member {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m.isLeader() {
				return m
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// waitForNewLeader looks for a leader other than exclude, in a term
// strictly greater than afterTerm -- Raft's Election Safety property
// guarantees at most one leader per term, so a higher term is authoritative
// over a possibly-stale claim from a partitioned or about-to-die node.
func waitForNewLeader(members []*member, exclude *member, afterTerm uint64, timeout time.Duration) *member {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m == exclude {
				continue
			}
			if m.isLeader() && m.term() > afterTerm {
				return m
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func put(t *testing.T, m *member, key, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, m.url()+"/v1/kv/"+key, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("building PUT request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT to %s: %v", m.id, err)
	}
	return resp
}

func get(t *testing.T, m *member, key string) *http.Response {
	t.Helper()
	resp, err := http.Get(m.url() + "/v1/kv/" + key)
	if err != nil {
		t.Fatalf("GET from %s: %v", m.id, err)
	}
	return resp
}
