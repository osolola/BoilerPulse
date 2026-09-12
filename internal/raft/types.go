// Package raft implements the Raft consensus algorithm: leader election and
// log replication, per the Raft paper (Ongaro & Ousterhout, "In Search of
// an Understandable Consensus Algorithm"). Node is transport- and
// storage-agnostic — it depends only on the Transport, Storage, and
// StateMachine interfaces in interfaces.go — so the algorithm itself can be
// tested with a fast, deterministic in-memory network (see node_test.go)
// independently of the real gRPC transport used in production
// (internal/raft/rpc) and the file-backed persistence (filestorage.go).
//
// Log compaction via snapshotting is implemented (see node.go's
// logBaseIndex/logBaseTerm and apply.go's maybeSnapshot): once applied
// entries pass Options.SnapshotThreshold, the state machine is snapshotted
// and the covered log prefix is discarded, both in memory and on disk. A
// follower too far behind to catch up via AppendEntries receives the whole
// snapshot in one InstallSnapshot RPC instead (not chunked -- see
// docs/raft.md for why that's an acceptable simplification here).
package raft

import "time"

// State is one of the three Raft server states.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// LogEntry is one entry in the replicated log. Index is 1-based; index 0
// means "no entry" (used as a sentinel for an empty log).
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// RequestVoteArgs is the RequestVote RPC's request.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC's response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC's request. Entries is empty
// for a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC's response. ConflictIndex is
// set on failure as a hint for the leader to back off nextIndex directly to
// that value, rather than one entry at a time (an optimization the Raft
// paper's extended version mentions).
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
}

// InstallSnapshotArgs is the InstallSnapshot RPC's request: the leader's
// entire snapshot, sent in one piece.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply is the InstallSnapshot RPC's response.
type InstallSnapshotReply struct {
	Term uint64
}

// Options tunes election/heartbeat timing. Smaller values make tests fast;
// production should use timings with enough margin over real network RTT.
type Options struct {
	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
	RPCTimeout         time.Duration
	// SnapshotThreshold is how many entries may accumulate beyond the last
	// snapshot (measured in applied entries) before the state machine is
	// snapshotted and the covered log prefix discarded. <= 0 disables
	// snapshotting entirely -- the log grows unboundedly, matching this
	// project's original (pre-snapshotting) behavior. Algorithm tests
	// default to 0 (via testOptions in testutil_test.go) so existing
	// election/replication tests are unaffected; tests that specifically
	// exercise snapshotting set this explicitly.
	SnapshotThreshold int
}

// DefaultOptions returns production-sized timing.
func DefaultOptions() Options {
	return Options{
		MinElectionTimeout: 300 * time.Millisecond,
		MaxElectionTimeout: 600 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
		TickInterval:       10 * time.Millisecond,
		RPCTimeout:         200 * time.Millisecond,
		// 1000 entries was chosen empirically: docs/benchmarking.md's real
		// runs put a single write-heavy scenario in the low hundreds of
		// entries, and the raft-log.bin that actually stalled writes after
		// days of manual testing (see docs/failure-testing.md) held far
		// more than that -- this keeps the log small without snapshotting
		// on every handful of writes.
		SnapshotThreshold: 1000,
	}
}

// Status is a snapshot of a Node's state, for observability (e.g. the
// /v1/cluster HTTP endpoint).
type Status struct {
	ID           string
	State        State
	Term         uint64
	LeaderID     string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
}
