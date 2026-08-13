// Package rpc adapts internal/raft's transport-agnostic Node to real gRPC:
// Transport (this file) is the client side, Server (server.go) is the
// server side. Both convert between raft's plain Go structs and the
// generated protobuf types in pkg/protocol/raftpb — internal/raft itself
// never imports gRPC or protobuf.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"boilerpulse/internal/raft"
	"boilerpulse/pkg/protocol/raftpb"
)

var _ raft.Transport = (*Transport)(nil)

// ErrFaultInjected is returned by Send* when a chaos fault (partition or a
// drop-rate roll) is currently blocking outgoing RPCs — see Faults.
var ErrFaultInjected = errors.New("rpc: request dropped by injected fault")

// Transport implements raft.Transport over gRPC, lazily dialing and caching
// one connection per peer address. faults (if non-nil) is consulted before
// every outgoing RPC for chaos-testing (spec §23/§34).
type Transport struct {
	mu     sync.Mutex
	addrs  map[string]string // peer ID -> "host:port"
	conns  map[string]*grpc.ClientConn
	faults *Faults
}

// NewTransport builds a Transport that resolves peer IDs to addresses via
// addrs (e.g. {"node-2": "localhost:9081"}). faults may be nil, meaning no
// chaos is ever injected on the outgoing side.
func NewTransport(addrs map[string]string, faults *Faults) *Transport {
	return &Transport{
		addrs:  addrs,
		conns:  make(map[string]*grpc.ClientConn),
		faults: faults,
	}
}

func (t *Transport) clientFor(peer string) (raftpb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peer]; ok {
		return raftpb.NewRaftServiceClient(conn), nil
	}

	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("rpc: no address configured for peer %q", peer)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("rpc: dialing peer %q at %s: %w", peer, addr, err)
	}
	t.conns[peer] = conn
	return raftpb.NewRaftServiceClient(conn), nil
}

func (t *Transport) SendRequestVote(ctx context.Context, peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if t.faults != nil && !t.faults.apply() {
		return nil, ErrFaultInjected
	}

	client, err := t.clientFor(peer)
	if err != nil {
		return nil, err
	}

	resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *Transport) SendAppendEntries(ctx context.Context, peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if t.faults != nil && !t.faults.apply() {
		return nil, ErrFaultInjected
	}

	client, err := t.clientFor(peer)
	if err != nil {
		return nil, err
	}

	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}

	resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

// Close closes every dialed connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for _, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
