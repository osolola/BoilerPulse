package raft

import "context"

// HandleAppendEntries processes an incoming AppendEntries RPC (heartbeat or
// log replication). Safe for concurrent use.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		reply.Success = false
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.state == Candidate {
		n.becomeFollowerLocked(args.Term) // same term, but a leader already exists
	}
	n.state = Follower // any valid AppendEntries from a current-term leader keeps us a follower
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Term = n.currentTerm

	// Consistency check: reject unless our log has an entry at PrevLogIndex
	// whose term matches PrevLogTerm.
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex < n.logBaseIndex {
			// The leader thinks we still need an entry we've already
			// compacted into our own snapshot -- tell it to skip straight
			// to right after our snapshot instead of walking
			// PrevLogIndex down towards zero (which we could never
			// satisfy: those entries are gone for good, replaced by the
			// snapshot).
			reply.ConflictIndex = n.logBaseIndex + 1
			return reply
		}
		if args.PrevLogIndex > n.lastLogIndexLocked() {
			reply.ConflictIndex = n.lastLogIndexLocked() + 1
			return reply
		}
		if got := n.termAtLocked(args.PrevLogIndex); got != args.PrevLogTerm {
			conflictTerm := got
			idx := args.PrevLogIndex
			for idx > 1 && n.termAtLocked(idx-1) == conflictTerm {
				idx--
			}
			reply.ConflictIndex = idx
			return reply
		}
	}

	// Append any entries not already present; truncate on the first
	// conflict (same index, different term) and append the leader's
	// version from there. Entries already present with matching terms are
	// left alone.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastLogIndexLocked() {
			if n.termAtLocked(idx) == e.Term {
				continue
			}
			n.truncateLogFromLocked(idx)
			n.appendLogLocked(args.Entries[i:])
			break
		}
		n.appendLogLocked(args.Entries[i:])
		break
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
		n.signalApply()
	}

	reply.Success = true
	return reply
}

// replicationLoop is the sole sender of AppendEntries to one peer for the
// lifetime of the Node, started by Start and stopped by Stop. Serializing
// sends this way (rather than spawning a goroutine per peer per proposal,
// as an earlier version of this code did) means a burst of concurrent
// Propose calls coalesces into however many RPCs the peer can actually
// keep up with: extra triggerReplication calls that arrive while a send is
// already in flight are simply dropped, and the next send always reads
// fresh log/commit state, so nothing proposed is ever lost by coalescing.
// Without this, concurrent overlapping sends to the same peer could starve
// that peer's heartbeats behind a pile of redundant, ever-growing
// AppendEntries RPCs, causing it to time out and start an unnecessary
// election even though the leader was alive and making progress the whole
// time — a real instability found via cmd/simulator load testing, not
// theoretical (see docs/benchmarking.md).
func (n *Node) replicationLoop(peer string) {
	defer n.replicationWG.Done()
	ch := n.replicateCh[peer]
	for {
		select {
		case <-n.stopCh:
			return
		case <-ch:
			n.sendAppendEntriesTo(peer)
		}
	}
}

// triggerReplication asks peer's replicationLoop to send an AppendEntries
// as soon as it's free. Non-blocking and coalescing — see replicationLoop.
func (n *Node) triggerReplication(peer string) {
	select {
	case n.replicateCh[peer] <- struct{}{}:
	default:
	}
}

// maxReplicationBatchSize caps how many entries one AppendEntries RPC
// carries -- see its use in sendAppendEntriesTo.
const maxReplicationBatchSize = 64

// sendAppendEntriesTo replicates (or heartbeats) to one peer. It must not
// be called while holding n.mu, and must only be called from peer's own
// replicationLoop goroutine (never concurrently for the same peer).
func (n *Node) sendAppendEntriesTo(peer string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	nextIdx := n.nextIndex[peer]
	if nextIdx == 0 {
		nextIdx = 1
	}

	if nextIdx <= n.logBaseIndex {
		// The entries this peer needs have already been compacted away by
		// a snapshot on this leader -- there's no valid AppendEntries to
		// send (PrevLogIndex would reference an entry that no longer
		// exists). Send the snapshot itself instead.
		n.mu.Unlock()
		n.sendInstallSnapshotTo(peer, term)
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := n.termAtLocked(prevLogIndex)

	var entries []LogEntry
	if nextIdx <= n.lastLogIndexLocked() {
		tail := n.log[nextIdx-n.logBaseIndex-1:]
		if len(tail) > maxReplicationBatchSize {
			// A follower that's fallen far behind (e.g. it was just
			// disconnected, or the leader's own group-commit just
			// coalesced a large burst of proposals into one jump in the
			// log) gets caught up incrementally, not in a single RPC big
			// enough to risk blowing past RPCTimeout -- a timed-out send
			// never advances nextIndex, so an uncapped send here just
			// retries the same (by then even larger) backlog forever
			// instead of making any progress. A real, measured regression
			// an earlier version of this batching work introduced --
			// see docs/benchmarking.md.
			tail = tail[:maxReplicationBatchSize]
		}
		entries = append(entries, tail...) // copied out while still locked
	}
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.opts.RPCTimeout)
	defer cancel()

	reply, err := n.transport.SendAppendEntries(ctx, peer, &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return // peer unreachable; the next tick will retry
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return // stale: we've moved on since sending this request
	}

	if reply.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = newMatch + 1
		n.maybeAdvanceCommitIndexLocked()
		return
	}

	if reply.ConflictIndex > 0 {
		n.nextIndex[peer] = reply.ConflictIndex
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
}

// maybeAdvanceCommitIndexLocked implements the Raft paper §5.4.2 rule: a
// leader only commits an entry by counting replicas for entries from its
// OWN current term. Older-term entries are committed only indirectly, once
// a current-term entry that comes after them (and thus covers them, by the
// log matching property) is itself committed.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	if n.state != Leader {
		return
	}
	for idx := n.lastLogIndexLocked(); idx > n.commitIndex; idx-- {
		if n.termAtLocked(idx) != n.currentTerm {
			continue
		}
		count := 1 // the leader itself
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.signalApply()
			return
		}
	}
}

// sendInstallSnapshotTo sends the leader's current snapshot to peer, used
// when peer's nextIndex has fallen behind the leader's own compacted log
// prefix (see sendAppendEntriesTo). Must not be called while holding n.mu.
//
// The whole snapshot goes in one RPC, not chunked -- a real production
// Raft would chunk a large snapshot to bound per-RPC memory and avoid
// re-sending everything on a mid-transfer failure, but for this project's
// scale (a demo KV dataset, not gigabytes of state) that would be
// complexity without a payoff. See docs/raft.md.
func (n *Node) sendInstallSnapshotTo(peer string, term uint64) {
	lastIncludedIndex, lastIncludedTerm, data, ok, err := n.storage.LoadSnapshot()
	if err != nil {
		n.logger.Error("failed to load snapshot for InstallSnapshot", "peer", peer, "error", err)
		return
	}
	if !ok {
		// nextIndex[peer] <= logBaseIndex implies a snapshot exists; this
		// would mean storage is in an inconsistent state. Nothing to send
		// either way -- the next heartbeat cycle will retry.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.opts.RPCTimeout)
	defer cancel()

	reply, err := n.transport.SendInstallSnapshot(ctx, peer, &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	})
	if err != nil {
		return // peer unreachable; the next heartbeat retries
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return // stale: we've moved on since sending this request
	}

	if lastIncludedIndex+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = lastIncludedIndex + 1
	}
	if lastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = lastIncludedIndex
	}
	n.maybeAdvanceCommitIndexLocked()
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC: persists
// the snapshot, restores the state machine from it, and discards whatever
// log this node was holding. Safe for concurrent use.
//
// Unlike the Raft paper's suggested optimization of keeping a log suffix
// that already matches the snapshot's boundary, this always discards the
// entire local log rather than trying to preserve one. That costs a
// handful of already-known entries potentially being re-sent afterward --
// a performance concession only, never a correctness one -- in exchange for
// meaningfully simpler code. Matches this project's general preference for
// simple-and-correct over optimal (see docs/storage-engine.md's own
// "known simplifications" for the same tradeoff made elsewhere).
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()

	if args.Term < n.currentTerm {
		reply := &InstallSnapshotReply{Term: n.currentTerm}
		n.mu.Unlock()
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	term := n.currentTerm

	if args.LastIncludedIndex <= n.logBaseIndex {
		// A stale or duplicate retransmit of a snapshot we've already
		// installed (or surpassed via normal replication since) -- ack
		// without doing anything.
		n.mu.Unlock()
		return &InstallSnapshotReply{Term: term}
	}
	n.mu.Unlock()

	// Persisting and restoring run unlocked -- a full-dataset restore can
	// be slow, and there's nothing else that needs n.mu held for either
	// step (both act entirely on storage/the state machine, not Node
	// fields).
	if err := n.storage.SaveSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
		n.logger.Error("failed to persist installed snapshot", "error", err)
		return &InstallSnapshotReply{Term: term}
	}
	if err := n.stateMachine.Restore(args.Data); err != nil {
		n.logger.Error("failed to restore state machine from installed snapshot", "error", err)
		return &InstallSnapshotReply{Term: term}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if args.LastIncludedIndex <= n.logBaseIndex {
		// Raced with something else (e.g. a second InstallSnapshot, or
		// enough normal replication) that already moved us past this one
		// while the restore above ran unlocked -- don't regress.
		return &InstallSnapshotReply{Term: n.currentTerm}
	}

	n.log = nil
	if err := n.storage.DiscardLogThrough(args.LastIncludedIndex); err != nil {
		n.logger.Error("failed to discard log after installing snapshot", "error", err)
	}
	n.logBaseIndex = args.LastIncludedIndex
	n.logBaseTerm = args.LastIncludedTerm
	if n.logBaseIndex > n.commitIndex {
		n.commitIndex = n.logBaseIndex
	}
	if n.logBaseIndex > n.lastApplied {
		n.lastApplied = n.logBaseIndex
	}

	n.logger.Info("installed snapshot from leader", "leader", args.LeaderID, "through_index", n.logBaseIndex, "term", n.logBaseTerm)
	return &InstallSnapshotReply{Term: n.currentTerm}
}
