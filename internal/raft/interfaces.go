package raft

import "context"

// Transport sends RPCs to a named peer. Implementations may be in-memory
// (tests) or real gRPC (internal/raft/rpc, used by cmd/node).
type Transport interface {
	SendRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	// SendInstallSnapshot sends a whole snapshot to peer, used when peer's
	// nextIndex has fallen behind this node's compacted log prefix.
	SendInstallSnapshot(ctx context.Context, peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Storage persists Raft's durable state: the current term, the candidate
// voted for in that term, the log, and (once compaction has run at least
// once) the latest snapshot. Per the Raft paper, all of this must be
// persisted before a node responds to an RPC that changed it.
type Storage interface {
	SaveTermAndVote(term uint64, votedFor string) error
	LoadTermAndVote() (term uint64, votedFor string, err error)

	AppendEntries(entries []LogEntry) error
	// TruncateFrom discards every persisted entry with Index >= index --
	// used for conflict resolution (a follower's log diverges from the
	// leader's and must be overwritten from that point).
	TruncateFrom(index uint64) error
	// LoadLog returns every persisted entry, in order, for replay on
	// startup. After compaction, this only includes entries after the
	// latest snapshot's LastIncludedIndex -- older entries live in the
	// snapshot instead.
	LoadLog() ([]LogEntry, error)

	// SaveSnapshot atomically persists a snapshot of the state machine as
	// of lastIncludedIndex/lastIncludedTerm, replacing any previous one.
	// It does not touch the log -- callers discard the covered prefix
	// separately via DiscardLogThrough, only once the snapshot is safely
	// on disk (so a crash between the two leaves either the old snapshot
	// with the full log, or the new snapshot with the full log -- either
	// is recoverable; never a gap).
	SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error
	// LoadSnapshot returns the most recently saved snapshot. ok is false
	// if none has ever been saved (a brand-new node, or one that has never
	// compacted its log).
	LoadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, ok bool, err error)
	// DiscardLogThrough permanently removes every persisted log entry with
	// Index <= index. Callers must only call this after a snapshot
	// covering at least index has already been durably saved via
	// SaveSnapshot -- otherwise those entries would be unrecoverable.
	DiscardLogThrough(index uint64) error
}

// StateMachine is what committed log entries get applied to, in order,
// exactly once each. internal/storage.RaftStateMachine adapts a KV
// storage.Engine to this interface.
type StateMachine interface {
	Apply(command []byte) error
	// Snapshot returns a serialized copy of the state machine's entire
	// current state, suitable for a later Restore -- either by this same
	// node after a restart, or by a follower installed to via
	// InstallSnapshot. Only called when a Node is about to compact its
	// log, never on the hot write path, so it does not need to be fast.
	Snapshot() ([]byte, error)
	// Restore replaces the state machine's entire state with what data
	// encodes (as previously produced by Snapshot). Called once at
	// startup if a snapshot was loaded (before any log replay), and once
	// per InstallSnapshot RPC received.
	Restore(data []byte) error
}
