package raft

import (
	"context"
	"time"
)

// pendingPropose is one command waiting to be assigned a log index,
// appended, and durably persisted -- submitted to appendLoop for group
// commit (see its doc comment).
type pendingPropose struct {
	command []byte
	// doneCh receives exactly once. entry.Index/Term are always set, even
	// alongside a non-nil err, so a caller that got ErrNotLeader here still
	// knows what index its command (if it landed at all, on whichever node
	// was actually leader) would have needed.
	doneCh chan pendingResult
}

type pendingResult struct {
	entry LogEntry
	err   error
}

// Propose appends command to the log (if this node is currently the
// leader), triggers immediate replication, and blocks until it's committed
// and applied to the state machine, ctx is done, or this node stops being
// leader for that entry (e.g. it was overwritten by a different leader
// after a partition), in which case it returns ErrNotLeader — the caller
// should retry, likely against a different node.
func (n *Node) Propose(ctx context.Context, command []byte) error {
	if !n.IsLeader() {
		return ErrNotLeader // fast path; appendLoop re-checks authoritatively below
	}

	req := &pendingPropose{command: command, doneCh: make(chan pendingResult, 1)}
	select {
	case n.proposeCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrNotLeader
	}

	var result pendingResult
	select {
	case result = <-req.doneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	if result.err != nil {
		return result.err
	}

	for _, peer := range n.peers {
		n.triggerReplication(peer)
	}

	return n.waitForApply(ctx, result.entry.Index, result.entry.Term)
}

// appendLoop is the sole caller of appendLogLocked for leader-initiated
// proposals, started by Start and stopped by Stop. Like replicationLoop
// (see replication.go's doc comment on the same pattern), draining
// everything already queued on proposeCh before processing turns a burst
// of concurrent Propose calls into one batch instead of one append (and,
// critically, one fsync -- FileStorage.AppendEntries syncs once per call
// covering every entry it's given) per proposal.
//
// This is the actual fix for the write-throughput ceiling documented in
// docs/benchmarking.md: previously Propose held n.mu for the full duration
// of its own synchronous append-and-fsync, so concurrent proposals could
// never overlap their disk waits at all -- the Nth concurrent proposer
// couldn't even start its own fsync until the (N-1)th's had completely
// finished and released the lock. Here, many proposers can be queued
// waiting on their own doneCh while a single shared fsync (covering all of
// them at once) is in flight.
// maxAppendBatchSize bounds how many proposals appendLoop will coalesce
// into one append-and-fsync call (and so, transitively, how many entries a
// lagging follower might need in a single AppendEntries RPC once
// replication catches up to them -- see sendAppendEntriesTo's own cap).
// Uncapped, a big enough burst of concurrent proposals produced a single
// batch large enough that a follower's response routinely blew past
// RPCTimeout trying to persist it all in one RPC, and since a timed-out
// send never advances nextIndex, the SAME (now even larger, since more had
// piled up in the meantime) backlog got retried on the next heartbeat --
// a real, measured regression a first version of this batching introduced
// (see docs/benchmarking.md), not a theoretical concern.
const maxAppendBatchSize = 64

func (n *Node) appendLoop() {
	defer close(n.appendDoneCh)
	for {
		select {
		case <-n.stopCh:
			return
		case first := <-n.proposeCh:
			batch := []*pendingPropose{first}
		drain:
			for len(batch) < maxAppendBatchSize {
				select {
				case req := <-n.proposeCh:
					batch = append(batch, req)
				default:
					break drain
				}
			}
			n.appendBatch(batch)
		}
	}
}

// appendBatch assigns every request in batch a sequential index (in
// arrival order), appends them to the log in one call (and so, on
// FileStorage, with a single trailing fsync covering the whole batch), and
// reports each request's own entry back to it.
func (n *Node) appendBatch(batch []*pendingPropose) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		for _, req := range batch {
			req.doneCh <- pendingResult{err: ErrNotLeader}
		}
		return
	}

	term := n.currentTerm
	nextIndex := n.lastLogIndexLocked() + 1
	entries := make([]LogEntry, len(batch))
	for i, req := range batch {
		entries[i] = LogEntry{Term: term, Index: nextIndex + uint64(i), Command: req.command}
	}
	n.appendLogLocked(entries)
	n.mu.Unlock()

	for i, req := range batch {
		req.doneCh <- pendingResult{entry: entries[i]}
	}
}

func (n *Node) waitForApply(ctx context.Context, index, proposedTerm uint64) error {
	ticker := time.NewTicker(n.opts.TickInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		if index <= n.lastLogIndexLocked() && n.termAtLocked(index) != proposedTerm {
			// A different leader's entry now occupies this index -- our
			// proposal was overwritten and will never be applied.
			n.mu.Unlock()
			return ErrNotLeader
		}
		applied := n.lastApplied >= index
		stillLeader := n.state == Leader
		n.mu.Unlock()

		if applied {
			return nil
		}
		if !stillLeader {
			return ErrNotLeader
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n *Node) signalApply() {
	select {
	case n.applyNotify <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	defer close(n.applyDoneCh)
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyNotify:
			n.applyPending()
		}
	}
}

// applyPending applies every committed-but-not-yet-applied entry to the
// state machine, in order. It releases the lock before calling Apply, so a
// slow state machine doesn't block RPC handling or election ticking. Once
// caught up, it checks whether the log has grown enough since the last
// snapshot to compact again.
func (n *Node) applyPending() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			break
		}
		n.lastApplied++
		entry := n.log[n.lastApplied-n.logBaseIndex-1]
		n.mu.Unlock()

		if err := n.stateMachine.Apply(entry.Command); err != nil {
			n.logger.Error("state machine apply failed", "index", entry.Index, "error", err)
		}
	}
	n.maybeSnapshot()
}

// maybeSnapshot compacts the log if enough entries have been applied since
// the last snapshot (Options.SnapshotThreshold; <= 0 disables this
// entirely). Only ever called from applyLoop's single goroutine, so
// lastApplied cannot move again while this runs -- no other goroutine
// writes it. Snapshot() and SaveSnapshot() both run unlocked (a full
// state-machine scan can be slow), which is safe for the same reason.
func (n *Node) maybeSnapshot() {
	if n.opts.SnapshotThreshold <= 0 {
		return
	}

	n.mu.Lock()
	lastApplied := n.lastApplied
	logBaseIndex := n.logBaseIndex
	if lastApplied <= logBaseIndex || lastApplied-logBaseIndex < uint64(n.opts.SnapshotThreshold) {
		n.mu.Unlock()
		return
	}
	term := n.termAtLocked(lastApplied)
	n.mu.Unlock()

	data, err := n.stateMachine.Snapshot()
	if err != nil {
		n.logger.Error("failed to snapshot state machine", "through_index", lastApplied, "error", err)
		return
	}
	if err := n.storage.SaveSnapshot(lastApplied, term, data); err != nil {
		n.logger.Error("failed to persist snapshot", "through_index", lastApplied, "error", err)
		return
	}

	n.mu.Lock()
	n.discardLogPrefixLocked(lastApplied, term)
	n.mu.Unlock()

	n.logger.Info("compacted raft log via snapshot", "through_index", lastApplied, "term", term, "snapshot_bytes", len(data))
}
