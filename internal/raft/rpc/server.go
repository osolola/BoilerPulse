package rpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"boilerpulse/internal/raft"
	"boilerpulse/pkg/protocol/raftpb"
)

// Server implements raftpb.RaftServiceServer by converting protobuf
// messages to/from raft's plain Go structs and calling straight into a
// *raft.Node's RPC handlers. faults (if non-nil) is consulted before every
// incoming RPC for chaos-testing (spec §23/§34) — sharing the same *Faults
// instance as the node's own Transport is what makes a "partition" set on
// this node block traffic in both directions.
type Server struct {
	raftpb.UnimplementedRaftServiceServer
	node   *raft.Node
	faults *Faults
}

// NewServer wraps node as a gRPC-servable RaftServiceServer. faults may be
// nil, meaning no chaos is ever injected on the incoming side.
func NewServer(node *raft.Node, faults *Faults) *Server {
	return &Server{node: node, faults: faults}
}

func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	if s.faults != nil && !s.faults.apply() {
		return nil, status.Error(codes.Unavailable, "rpc: request dropped by injected fault")
	}

	reply := s.node.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &raftpb.RequestVoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	if s.faults != nil && !s.faults.apply() {
		return nil, status.Error(codes.Unavailable, "rpc: request dropped by injected fault")
	}

	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}

	reply := s.node.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	return &raftpb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}
