package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"

	"boilerpulse/internal/api"
	"boilerpulse/internal/raft"
	raftrpc "boilerpulse/internal/raft/rpc"
	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/lsm"
	"boilerpulse/pkg/protocol/raftpb"
)

// clusterMember is one fully-wired node: real lsm.Engine (on a temp dir),
// real raft.Node with real gRPC transport/server, and the real KV HTTP API
// with a Proposer wired in -- the same components cmd/node assembles, built
// here directly so the test can drive everything over real HTTP without
// spawning subprocesses.
type clusterMember struct {
	id         string
	httpServer *httptest.Server
	raftNode   *raft.Node
	grpcServer *grpc.Server
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

func testRaftOptions() raft.Options {
	return raft.Options{
		MinElectionTimeout: 80 * time.Millisecond,
		MaxElectionTimeout: 160 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		TickInterval:       5 * time.Millisecond,
		RPCTimeout:         200 * time.Millisecond,
	}
}

// startCluster wires up n real, fully-functional nodes, each with its own
// temp data directory, real gRPC Raft transport, and real HTTP KV API.
func startCluster(t *testing.T, n int) []*clusterMember {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i+1)
	}

	// Phase 1: grab a listener (and therefore a known address) for every
	// node before constructing anything that needs to know every peer's
	// address up front.
	listeners := make(map[string]net.Listener, n)
	addrs := make(map[string]string, n)
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen(%s): %v", id, err)
		}
		listeners[id] = lis
		addrs[id] = lis.Addr().String()
	}

	// Phase 2: build each node now that the full address map is known.
	members := make([]*clusterMember, n)
	for i, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		engine, err := lsm.Open(t.TempDir(), logger, lsm.DefaultOptions())
		if err != nil {
			t.Fatalf("lsm.Open(%s): %v", id, err)
		}
		t.Cleanup(func() { engine.Close() })

		raftStorage, err := raft.OpenFileStorage(t.TempDir())
		if err != nil {
			t.Fatalf("OpenFileStorage(%s): %v", id, err)
		}
		t.Cleanup(func() { raftStorage.Close() })

		transport := raftrpc.NewTransport(addrs, nil) // no chaos faults in this test
		t.Cleanup(func() { transport.Close() })

		sm := storage.NewRaftStateMachine(engine)
		node, err := raft.NewNode(id, peers, raftStorage, transport, sm, logger, testRaftOptions())
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}

		grpcServer := grpc.NewServer()
		raftpb.RegisterRaftServiceServer(grpcServer, raftrpc.NewServer(node, nil))
		go grpcServer.Serve(listeners[id])

		node.Start()

		apiServer := api.NewServer(engine, logger, id)
		apiServer.SetProposer(&memberProposer{node: node})
		httpServer := httptest.NewServer(apiServer)

		member := &clusterMember{id: id, httpServer: httpServer, raftNode: node, grpcServer: grpcServer}
		members[i] = member

		t.Cleanup(func() {
			httpServer.Close()
			node.Stop()
			grpcServer.Stop()
		})
	}

	return members
}

func waitForLeader(members []*clusterMember, timeout time.Duration) *clusterMember {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m.raftNode.IsLeader() {
				return m
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func TestThreeNodeClusterElectsLeaderAndReplicatesWrites(t *testing.T) {
	members := startCluster(t, 3)

	leader := waitForLeader(members, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 5s")
	}

	putBody := `{"value":{"title":"Purdue Basketball"},"consistency":"STRONG","ttl_seconds":3600}`
	req, err := http.NewRequest(http.MethodPut, leader.httpServer.URL+"/v1/kv/event:mackey", bytes.NewBufferString(putBody))
	if err != nil {
		t.Fatalf("building PUT request: %v", err)
	}
	resp, err := leader.httpServer.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT to leader (%s): %v", leader.id, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// A follower must reject the write and name the current leader.
	for _, m := range members {
		if m == leader {
			continue
		}
		followerReq, _ := http.NewRequest(http.MethodPut, m.httpServer.URL+"/v1/kv/should-fail", bytes.NewBufferString(`{"value":"x"}`))
		followerResp, err := m.httpServer.Client().Do(followerReq)
		if err != nil {
			t.Fatalf("PUT to follower (%s): %v", m.id, err)
		}
		followerResp.Body.Close()
		if followerResp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("PUT to follower %s status = %d, want %d", m.id, followerResp.StatusCode, http.StatusServiceUnavailable)
		}
		break
	}

	// The write must show up on every node, including followers.
	for _, m := range members {
		var value json.RawMessage
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			getResp, err := m.httpServer.Client().Get(m.httpServer.URL + "/v1/kv/event:mackey")
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if getResp.StatusCode == http.StatusOK {
				var body struct {
					Value json.RawMessage `json:"value"`
				}
				json.NewDecoder(getResp.Body).Decode(&body)
				getResp.Body.Close()
				value = body.Value
				break
			}
			getResp.Body.Close()
			time.Sleep(10 * time.Millisecond)
		}
		if value == nil {
			t.Fatalf("node %s never saw the replicated write", m.id)
		}
		if string(value) != `{"title":"Purdue Basketball"}` {
			t.Errorf("node %s value = %s, want the leader's write", m.id, value)
		}
	}
}

func TestThreeNodeClusterFailsOverAndContinuesAcceptingWrites(t *testing.T) {
	members := startCluster(t, 3)

	leader := waitForLeader(members, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 5s")
	}
	firstTerm := leader.raftNode.Term()

	leader.raftNode.Stop()
	leader.grpcServer.Stop()
	leader.httpServer.Close()

	var newLeader *clusterMember
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m == leader {
				continue
			}
			if m.raftNode.IsLeader() && m.raftNode.Term() > firstTerm {
				newLeader = m
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after the original leader was stopped")
	}

	req, err := http.NewRequest(http.MethodPut, newLeader.httpServer.URL+"/v1/kv/after-failover", bytes.NewBufferString(`{"value":"still-works"}`))
	if err != nil {
		t.Fatalf("building PUT request: %v", err)
	}
	resp, err := newLeader.httpServer.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT to new leader: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT to new leader status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}
