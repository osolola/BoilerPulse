package rpc

import (
	"context"
	"encoding/json"
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
	mu        sync.Mutex
	term      uint64
	votedFor  string
	log       []raft.LogEntry
	baseIndex uint64

	hasSnapshot   bool
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte
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

func (s *memStorage) position(index uint64) (int, bool) {
	if index <= s.baseIndex {
		return 0, false
	}
	pos := index - s.baseIndex - 1
	if pos >= uint64(len(s.log)) {
		return 0, false
	}
	return int(pos), true
}

func (s *memStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos, ok := s.position(index)
	if !ok {
		return nil
	}
	s.log = s.log[:pos]
	return nil
}

func (s *memStorage) LoadLog() ([]raft.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]raft.LogEntry, len(s.log))
	copy(out, s.log)
	return out, nil
}

func (s *memStorage) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hasSnapshot = true
	s.snapshotIndex, s.snapshotTerm = lastIncludedIndex, lastIncludedTerm
	s.snapshotData = append([]byte(nil), data...)
	return nil
}

func (s *memStorage) LoadSnapshot() (uint64, uint64, []byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasSnapshot {
		return 0, 0, nil, false, nil
	}
	return s.snapshotIndex, s.snapshotTerm, append([]byte(nil), s.snapshotData...), true, nil
}

func (s *memStorage) DiscardLogThrough(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.baseIndex {
		return nil
	}
	drop := index - s.baseIndex
	if drop > uint64(len(s.log)) {
		drop = uint64(len(s.log))
	}
	s.log = append([]raft.LogEntry(nil), s.log[drop:]...)
	s.baseIndex = index
	return nil
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

func (sm *testSM) Snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return json.Marshal(sm.applied)
}

func (sm *testSM) Restore(data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var applied [][]byte
	if err := json.Unmarshal(data, &applied); err != nil {
		return err
	}
	sm.applied = applied
	return nil
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

// TestInstallSnapshotOverRealGRPC exercises the actual protobuf encoding
// path (raftpb.InstallSnapshotRequest/Response), which the fake in-memory
// network used by internal/raft's own algorithm tests never touches at
// all -- a real encoding bug (e.g. a mismapped field) would only show up
// here.
func TestInstallSnapshotOverRealGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	sm := &testSM{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	node, err := raft.NewNode("follower", []string{"leader"}, &memStorage{}, NewTransport(nil, nil), sm, logger, testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, NewServer(node, nil))
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client := NewTransport(map[string]string{"follower": lis.Addr().String()}, nil)
	defer client.Close()

	snapshotData, err := json.Marshal([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatalf("marshal snapshot data: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := client.SendInstallSnapshot(ctx, "follower", &raft.InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "leader",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              snapshotData,
	})
	if err != nil {
		t.Fatalf("SendInstallSnapshot: %v", err)
	}
	if reply.Term != 3 {
		t.Errorf("reply.Term = %d, want 3 (follower had no prior term, should adopt the leader's)", reply.Term)
	}

	status := node.Status()
	if status.LastLogIndex != 10 {
		t.Errorf("Status().LastLogIndex = %d, want 10", status.LastLogIndex)
	}
	if status.CommitIndex != 10 || status.LastApplied != 10 {
		t.Errorf("Status() = {CommitIndex: %d, LastApplied: %d}, want both 10", status.CommitIndex, status.LastApplied)
	}

	applied := sm.Applied()
	if len(applied) != 3 || string(applied[0]) != "a" || string(applied[1]) != "b" || string(applied[2]) != "c" {
		t.Errorf("state machine after real-gRPC InstallSnapshot = %v, want [a b c]", applied)
	}
}
