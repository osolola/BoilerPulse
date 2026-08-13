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
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.termAtLocked(prevLogIndex)

	var entries []LogEntry
	if nextIdx <= n.lastLogIndexLocked() {
		entries = append(entries, n.log[nextIdx-1:]...) // copied out while still locked
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
