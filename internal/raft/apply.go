package raft

import (
	"context"
	"time"
)

// Propose appends command to the log (if this node is currently the
// leader), triggers immediate replication, and blocks until it's committed
// and applied to the state machine, ctx is done, or this node stops being
// leader for that entry (e.g. it was overwritten by a different leader
// after a partition), in which case it returns ErrNotLeader — the caller
// should retry, likely against a different node.
func (n *Node) Propose(ctx context.Context, command []byte) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.currentTerm
	entry := LogEntry{Term: term, Index: n.lastLogIndexLocked() + 1, Command: command}
	n.appendLogLocked([]LogEntry{entry})
	targetIndex := entry.Index
	n.mu.Unlock()

	for _, peer := range n.peers {
		n.triggerReplication(peer)
	}

	return n.waitForApply(ctx, targetIndex, term)
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
