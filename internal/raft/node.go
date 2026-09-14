package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ErrNotLeader is returned by Propose when this node isn't (or stops being)
// the current leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Node is one participant in a Raft cluster. All state is guarded by mu; a
// single background goroutine (run) drives ticking, and a second (applyLoop)
// applies committed entries to the state machine. RPC handlers (called by
// Transport's server side) and Propose (called by callers like
// internal/api) may run on arbitrary goroutines.
type Node struct {
	mu sync.Mutex

	id    string
	peers []string

	storage      Storage
	transport    Transport
	stateMachine StateMachine
	logger       *slog.Logger
	opts         Options

	// Persistent state (mirrored in memory; written through to storage
	// before any RPC reply that depends on a change to it).
	currentTerm uint64
	votedFor    string
	log         []LogEntry // log[i] has Index == logBaseIndex+i+1 (see log.go)

	// logBaseIndex/logBaseTerm describe the most recent snapshot: every
	// entry with Index <= logBaseIndex has been compacted out of log and
	// storage, its effect captured in the snapshot instead. Both are 0
	// (never set) for a node that has never snapshotted.
	logBaseIndex uint64
	logBaseTerm  uint64

	// Volatile state.
	state       State
	commitIndex uint64
	lastApplied uint64
	leaderID    string // best known leader ID; "" if unknown

	// Leader-only volatile state (reset on each new leadership term).
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionResetAt   time.Time
	electionTimeout   time.Duration
	lastHeartbeatSent time.Time

	applyNotify chan struct{}

	// replicateCh holds one buffered(1) trigger channel per peer, each
	// served by its own long-lived replicationLoop goroutine -- this
	// serializes AppendEntries sends per peer (see replication.go) so a
	// burst of concurrent Propose calls coalesces into however many RPCs
	// the peer can actually keep up with, instead of spawning one
	// goroutine per peer per proposal and flooding it with overlapping,
	// increasingly redundant sends (which starved heartbeats and caused
	// real leader churn under write bursts -- see docs/benchmarking.md).
	replicateCh   map[string]chan struct{}
	replicationWG sync.WaitGroup

	// proposeCh is where Propose enqueues a pending append, and appendLoop
	// (the sole writer of n.log for leader-initiated proposals) drains it.
	// Buffered generously so a burst of concurrent Propose calls can pile
	// up waiting to be picked up rather than each blocking on the enqueue
	// itself -- see appendLoop's doc comment for why that's the whole
	// point (group-committing their fsyncs together).
	proposeCh chan *pendingPropose

	stopCh       chan struct{}
	doneCh       chan struct{}
	applyDoneCh  chan struct{}
	appendDoneCh chan struct{}
	started      bool
	stopOnce     sync.Once
}

// NewNode constructs a Node, recovering currentTerm/votedFor/log from
// storage. Call Start to begin participating in the cluster.
func NewNode(id string, peers []string, storage Storage, transport Transport, sm StateMachine, logger *slog.Logger, opts Options) (*Node, error) {
	term, votedFor, err := storage.LoadTermAndVote()
	if err != nil {
		return nil, fmt.Errorf("loading persisted term/vote: %w", err)
	}

	// A snapshot, if one exists, must be restored into the state machine
	// before the surviving log tail is even considered -- LoadLog() only
	// returns what's after the snapshot's LastIncludedIndex to begin with
	// (the snapshot IS the rest of history). commitIndex/lastApplied start
	// at the snapshot's boundary rather than 0: everything up to there is
	// already reflected in the restored state machine, exactly as if it
	// had been applied entry by entry.
	var logBaseIndex, logBaseTerm, commitIndex, lastApplied uint64
	lastIncludedIndex, lastIncludedTerm, snapshotData, hasSnapshot, err := storage.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("loading persisted snapshot: %w", err)
	}
	if hasSnapshot {
		if err := sm.Restore(snapshotData); err != nil {
			return nil, fmt.Errorf("restoring state machine from snapshot: %w", err)
		}
		logBaseIndex, logBaseTerm = lastIncludedIndex, lastIncludedTerm
		commitIndex, lastApplied = lastIncludedIndex, lastIncludedIndex
	}

	log, err := storage.LoadLog()
	if err != nil {
		return nil, fmt.Errorf("loading persisted log: %w", err)
	}

	replicateCh := make(map[string]chan struct{}, len(peers))
	for _, p := range peers {
		replicateCh[p] = make(chan struct{}, 1)
	}
	const proposeChBuffer = 1024

	return &Node{
		id:           id,
		peers:        peers,
		storage:      storage,
		transport:    transport,
		stateMachine: sm,
		logger:       logger,
		opts:         opts,
		currentTerm:  term,
		votedFor:     votedFor,
		log:          log,
		logBaseIndex: logBaseIndex,
		logBaseTerm:  logBaseTerm,
		commitIndex:  commitIndex,
		lastApplied:  lastApplied,
		state:        Follower,
		applyNotify:  make(chan struct{}, 1),
		replicateCh:  replicateCh,
		proposeCh:    make(chan *pendingPropose, proposeChBuffer),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		applyDoneCh:  make(chan struct{}),
		appendDoneCh: make(chan struct{}),
	}, nil
}

// Start begins the election-timer/heartbeat loop and the apply loop.
func (n *Node) Start() {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.resetElectionTimerLocked()
	n.lastHeartbeatSent = time.Now()
	n.mu.Unlock()

	go n.run()
	go n.applyLoop()
	go n.appendLoop()
	for _, p := range n.peers {
		n.replicationWG.Add(1)
		go n.replicationLoop(p)
	}
}

// Stop halts every background loop (ticking, apply, and one per peer for
// replication) and waits for them to exit. It does not close Storage — the
// caller owns that. Safe to call more than once, and safe to call even if
// Start was never called.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.mu.Lock()
		started := n.started
		n.mu.Unlock()
		if started {
			<-n.doneCh
			<-n.applyDoneCh
			<-n.appendDoneCh
			n.replicationWG.Wait()
		}
	})
}

func (n *Node) ID() string { return n.id }

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// Leader returns the best-known current leader's ID, if any.
func (n *Node) Leader() (id string, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID, n.leaderID != ""
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Status returns a point-in-time snapshot for observability.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:           n.id,
		State:        n.state,
		Term:         n.currentTerm,
		LeaderID:     n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.lastLogIndexLocked(),
	}
}

func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) persistTermAndVoteLocked() {
	if err := n.storage.SaveTermAndVote(n.currentTerm, n.votedFor); err != nil {
		n.logger.Error("failed to persist term/vote", "error", err)
	}
}

// becomeFollowerLocked steps down to Follower. If term is newer than what
// we have, it also adopts that term and clears our vote (a new term means
// we haven't voted in it yet). The caller is responsible for setting
// leaderID afterward if it knows the new leader (e.g. from an AppendEntries
// sender); otherwise it's cleared to "unknown".
func (n *Node) becomeFollowerLocked(term uint64) {
	n.state = Follower
	n.leaderID = ""
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistTermAndVoteLocked()
	}
}
