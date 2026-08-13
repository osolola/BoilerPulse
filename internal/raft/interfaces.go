package raft

import "context"

// Transport sends RPCs to a named peer. Implementations may be in-memory
// (tests) or real gRPC (internal/raft/rpc, used by cmd/node).
type Transport interface {
	SendRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Storage persists Raft's durable state: the current term, the candidate
// voted for in that term, and the log. Per the Raft paper, all of this must
// be persisted before a node responds to an RPC that changed it.
type Storage interface {
	SaveTermAndVote(term uint64, votedFor string) error
	LoadTermAndVote() (term uint64, votedFor string, err error)

	AppendEntries(entries []LogEntry) error
	// TruncateFrom discards every persisted entry with Index >= index.
	TruncateFrom(index uint64) error
	// LoadLog returns every persisted entry, in order, for replay on startup.
	LoadLog() ([]LogEntry, error)
}

// StateMachine is what committed log entries get applied to, in order,
// exactly once each. internal/storage.RaftStateMachine adapts a KV
// storage.Engine to this interface.
type StateMachine interface {
	Apply(command []byte) error
}
