package raft

// Log helpers. n.log is 0-indexed with n.log[i].Index == i+1 (Raft indices
// are 1-based; index 0 means "no entry"). There's no snapshotting, so the
// log is always dense from index 1.

func (n *Node) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAtLocked(index uint64) uint64 {
	if index == 0 || index > n.lastLogIndexLocked() {
		return 0
	}
	return n.log[index-1].Term
}

// appendLogLocked appends entries to the in-memory log and persists them.
func (n *Node) appendLogLocked(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	n.log = append(n.log, entries...)
	if err := n.storage.AppendEntries(entries); err != nil {
		// The in-memory log (used for all replication decisions) and disk
		// can only diverge here on an I/O error. We log and continue
		// rather than crash, on the theory that a transient disk error
		// shouldn't take a whole node down mid-operation — but note this
		// as a known gap: a node that hits this repeatedly should restart
		// so LoadLog() re-syncs from what's actually durable on disk.
		n.logger.Error("failed to persist log entries", "error", err)
	}
}

// truncateLogFromLocked discards every entry with Index >= index, in both
// the in-memory log and storage.
func (n *Node) truncateLogFromLocked(index uint64) {
	if index == 0 || index > n.lastLogIndexLocked() {
		return
	}
	n.log = n.log[:index-1]
	if err := n.storage.TruncateFrom(index); err != nil {
		n.logger.Error("failed to truncate persisted log", "error", err)
	}
}
