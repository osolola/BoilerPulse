package rpc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"boilerpulse/internal/raft"
	"boilerpulse/pkg/protocol/raftpb"
)

type memStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	log      []raft.LogEntry
}

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

func (s *memStorage) AppendEntries(entries []raft.LogEntry) error {
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

func (s *memStorage) LoadLog() ([]raft.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]raft.LogEntry, len(s.log))
	copy(out, s.log)
	return out, nil
}

type testSM struct {
	mu      sync.Mutex
	applied [][]byte
}

func (sm *testSM) Apply(cmd []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.applied = append(sm.applied, append([]byte(nil), cmd...))
	return nil
}

func (sm *testSM) Applied() [][]byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make([][]byte, len(sm.applied))
	copy(out, sm.applied)
	return out
}

func testOptions() raft.Options {
	return raft.Options{
		MinElectionTimeout: 80 * time.Millisecond,
		MaxElectionTimeout: 160 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		TickInterval:       5 * time.Millisecond,
		RPCTimeout:         200 * time.Millisecond,
	}
}

// rpcNode bundles a raft.Node with the real gRPC listener/server serving it.
type rpcNode struct {
	node       *raft.Node
	grpcServer *grpc.Server
	addr       string
	sm         *testSM
	transport  *Transport
}

func startRPCNode(t *testing.T, id string, peerIDs []string) *rpcNode {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	sm := &testSM{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	transport := NewTransport(nil, nil) // addresses filled in by wireCluster once every node has a port; no chaos faults in this test
	node, err := raft.NewNode(id, peerIDs, &memStorage{}, transport, sm, logger, testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, NewServer(node, nil))
	go grpcServer.Serve(lis)

	return &rpcNode{node: node, grpcServer: grpcServer, addr: lis.Addr().String(), sm: sm, transport: transport}
}

// wireCluster starts n real gRPC+Raft nodes on localhost, resolves every
// transport's peer-address map now that all ports are known, and starts
// each Raft node's background loops.
func wireCluster(t *testing.T, n int) []*rpcNode {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = string(rune('A' + i))
	}

	nodes := make([]*rpcNode, n)
	for i, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		nodes[i] = startRPCNode(t, id, peers)
	}

	addrs := make(map[string]string, n)
	for i, id := range ids {
		addrs[id] = nodes[i].addr
	}
	for _, rn := range nodes {
		rn.transport.addrs = addrs
	}

	for _, rn := range nodes {
		rn.node.Start()
	}

	t.Cleanup(func() {
		for _, rn := range nodes {
			rn.node.Stop()
			rn.grpcServer.Stop()
			rn.transport.Close()
		}
	})

	return nodes
}

func TestElectionAndReplicationOverRealGRPC(t *testing.T) {
	nodes := wireCluster(t, 3)

	var leader *rpcNode
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, rn := range nodes {
			if rn.node.IsLeader() {
				leader = rn
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected over real gRPC within 3s")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.node.Propose(ctx, []byte("hello-over-grpc")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	for _, rn := range nodes {
		applied := waitForApplied(t, rn.sm, 3*time.Second)
		if len(applied) == 0 || string(applied[len(applied)-1]) != "hello-over-grpc" {
			t.Errorf("node %s applied = %v, want last entry %q", rn.node.ID(), applied, "hello-over-grpc")
		}
	}
}

func waitForApplied(t *testing.T, sm *testSM, timeout time.Duration) [][]byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if applied := sm.Applied(); len(applied) > 0 {
			return applied
		}
		time.Sleep(5 * time.Millisecond)
	}
	return sm.Applied()
}
