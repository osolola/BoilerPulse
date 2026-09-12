package raft

// Log helpers. n.log holds only entries after the last snapshot (or the
// whole log, if there's never been one): n.log[i].Index == n.logBaseIndex+i+1.
// n.logBaseIndex is 0 (and n.logBaseTerm meaningless) for a node that has
// never snapshotted, in which case this reduces to the original dense,
// 1-based-from-the-start scheme.

func (n *Node) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return n.logBaseIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return n.logBaseTerm
	}
	return n.log[len(n.log)-1].Term
}

// termAtLocked returns the term of the entry at index, or 0 if index is out
// of range (0, beyond the log, or older than what's still retained --
// callers needing an entry that old should be sending a snapshot instead,
// see sendAppendEntriesTo).
func (n *Node) termAtLocked(index uint64) uint64 {
	if index == 0 || index > n.lastLogIndexLocked() {
		return 0
	}
	if index == n.logBaseIndex {
		return n.logBaseTerm
	}
	if index < n.logBaseIndex {
		return 0
	}
	return n.log[index-n.logBaseIndex-1].Term
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
	if index <= n.logBaseIndex || index > n.lastLogIndexLocked() {
		return
	}
	n.log = n.log[:index-n.logBaseIndex-1]
	if err := n.storage.TruncateFrom(index); err != nil {
		n.logger.Error("failed to truncate persisted log", "error", err)
	}
}

// discardLogPrefixLocked removes every entry with Index <= index from
// memory and persistent storage, and records index/term as the new log
// base -- called after a snapshot covering at least index has already been
// durably saved (maybeSnapshot), or after installing a snapshot received
// via InstallSnapshot (HandleInstallSnapshot). term is the term of the
// entry at index, needed so lastLogTermLocked/termAtLocked stay correct
// once the log no longer contains that entry itself.
func (n *Node) discardLogPrefixLocked(index, term uint64) {
	if index <= n.logBaseIndex {
		return // already compacted at least this far
	}
	if last := n.lastLogIndexLocked(); index > last {
		index = last // defensive clamp; shouldn't happen
	}
	drop := index - n.logBaseIndex
	if drop > uint64(len(n.log)) {
		drop = uint64(len(n.log))
	}
	// Copy rather than reslice so the dropped entries' backing array can
	// actually be garbage collected -- a reslice keeps the whole original
	// array alive as long as the smaller slice references it.
	n.log = append([]LogEntry(nil), n.log[drop:]...)
	n.logBaseIndex = index
	n.logBaseTerm = term
	if err := n.storage.DiscardLogThrough(index); err != nil {
		n.logger.Error("failed to discard compacted log prefix from storage", "index", index, "error", err)
	}
}
