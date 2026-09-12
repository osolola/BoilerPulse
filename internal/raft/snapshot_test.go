package raft

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// snapshotTestOptions mirrors testOptions but with a small SnapshotThreshold
// so a handful of proposals is enough to exercise compaction -- testOptions
// itself deliberately leaves this at 0 (disabled) so every other algorithm
// test's behavior is unaffected by snapshotting existing at all.
func snapshotTestOptions(threshold int) Options {
	opts := testOptions()
	opts.SnapshotThreshold = threshold
	return opts
}

func TestSnapshotCompactsLogOnceThresholdCrossed(t *testing.T) {
	tc := newTestClusterWithOptions(t, 3, snapshotTestOptions(5))
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })

	const proposals = 12
	for i := 0; i < proposals; i++ {
		leader := tc.leader()
		if leader == nil {
			t.Fatalf("lost the leader partway through proposing (at i=%d)", i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := leader.Propose(ctx, []byte("entry"))
		cancel()
		if err != nil {
			t.Fatalf("Propose(%d): %v", i, err)
		}
	}

	leader := tc.leader()
	if leader == nil {
		t.Fatal("no leader after proposing")
	}
	var leaderStorage *memStorage
	for i, n := range tc.nodes {
		if n == leader {
			leaderStorage = tc.storages[i]
		}
	}

	waitFor(t, time.Second, "the leader to compact its log via a snapshot", func() bool {
		_, _, _, ok, err := leaderStorage.LoadSnapshot()
		return err == nil && ok
	})

	lastIncludedIndex, _, _, _, err := leaderStorage.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if lastIncludedIndex == 0 {
		t.Error("snapshot exists but LastIncludedIndex = 0, want > 0")
	}

	log, err := leaderStorage.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	if len(log) >= proposals {
		t.Errorf("persisted log still has %d entries after compaction (%d proposed), want fewer -- the prefix should have been discarded", len(log), proposals)
	}
}

func TestNodeRestoresFromSnapshotOnRestart(t *testing.T) {
	// Build a snapshot the same way maybeSnapshot does: a state machine's
	// own Snapshot() output, saved alongside a post-snapshot log tail --
	// then verify a fresh NewNode (simulating a process restart) restores
	// the state machine and picks up commitIndex/lastApplied/logBaseIndex
	// from exactly that boundary, not from 0.
	source := &testStateMachine{}
	if err := source.Apply([]byte("a")); err != nil {
		t.Fatalf("Apply a: %v", err)
	}
	if err := source.Apply([]byte("b")); err != nil {
		t.Fatalf("Apply b: %v", err)
	}
	snapshotData, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	storage := newMemStorage()
	if err := storage.SaveSnapshot(2, 1, snapshotData); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := storage.AppendEntries([]LogEntry{{Term: 1, Index: 3, Command: []byte("c")}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	fresh := &testStateMachine{} // a genuinely empty state machine -- restoring is NewNode's job, not this test's
	n, err := NewNode("node-1", []string{"node-2", "node-3"}, storage, &fakeTransport{net: newFakeNetwork(), selfID: "node-1"}, fresh, testLogger(), testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if n.logBaseIndex != 2 || n.logBaseTerm != 1 {
		t.Errorf("logBaseIndex/logBaseTerm = %d/%d, want 2/1", n.logBaseIndex, n.logBaseTerm)
	}
	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 (the snapshot boundary)", n.commitIndex)
	}
	if n.lastApplied != 2 {
		t.Errorf("lastApplied = %d, want 2 (the snapshot boundary)", n.lastApplied)
	}
	if got := n.lastLogIndexLocked(); got != 3 {
		t.Errorf("lastLogIndexLocked() = %d, want 3 (snapshot boundary 2 + one post-snapshot entry)", got)
	}

	applied := fresh.Applied()
	if len(applied) != 2 || string(applied[0]) != "a" || string(applied[1]) != "b" {
		t.Errorf("state machine after restore = %v, want [a b] (restored from the snapshot before any log replay)", applied)
	}
}

func TestDisconnectedFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	tc := newTestClusterWithOptions(t, 3, snapshotTestOptions(5))
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })
	leader := tc.leader()

	var followerIdx int
	var follower *Node
	for i, n := range tc.nodes {
		if n != leader {
			followerIdx = i
			follower = n
			break
		}
	}
	// Disconnect before any entries exist, so this follower's nextIndex
	// never advances past 1 -- guaranteeing that once the leader compacts
	// past index 1, only InstallSnapshot (never AppendEntries) can catch
	// it up.
	tc.net.disconnect(follower.ID())

	const proposals = 12
	for i := 0; i < proposals; i++ {
		cur := tc.leader()
		if cur == nil {
			t.Fatalf("lost the leader partway through proposing (at i=%d)", i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := cur.Propose(ctx, []byte("entry"))
		cancel()
		if err != nil {
			t.Fatalf("Propose(%d): %v", i, err)
		}
	}

	leader = tc.leader()
	var leaderStorage *memStorage
	for i, n := range tc.nodes {
		if n == leader {
			leaderStorage = tc.storages[i]
		}
	}
	waitFor(t, time.Second, "the leader to compact past the disconnected follower's nextIndex", func() bool {
		_, _, _, ok, _ := leaderStorage.LoadSnapshot()
		return ok
	})

	tc.net.reconnect(follower.ID())

	followerSM := tc.sms[followerIdx]
	waitFor(t, 3*time.Second, "the reconnected follower to catch up to the leader's applied state", func() bool {
		return len(followerSM.Applied()) == proposals
	})

	// The only way a follower whose nextIndex was stuck at 1 can end up
	// with a full copy of state the leader compacted away is by actually
	// receiving and installing a snapshot -- confirm that's what happened,
	// not just that the end state happens to match some other way.
	followerStorage := tc.storages[followerIdx]
	if _, _, _, ok, err := followerStorage.LoadSnapshot(); err != nil {
		t.Fatalf("LoadSnapshot on follower: %v", err)
	} else if !ok {
		t.Error("follower caught up but never saved a snapshot -- expected InstallSnapshot to have run")
	}

	leaderApplied := tc.sms[0].Applied()
	for i := range tc.nodes {
		if got := tc.sms[i].Applied(); len(got) != len(leaderApplied) {
			continue // already asserted above for the follower; a stale slow node here would be a different bug
		} else {
			for j := range got {
				if string(got[j]) != string(leaderApplied[j]) {
					t.Errorf("node %d applied[%d] = %q, want %q", i, j, got[j], leaderApplied[j])
				}
			}
		}
	}
}

// TestHandleInstallSnapshotIgnoresStaleRetransmit guards the idempotency
// case explicitly (HandleInstallSnapshot's own doc comment claims it): a
// retransmitted or reordered InstallSnapshot for an index we've already
// moved past must be a no-op, not regress logBaseIndex or re-restore state.
func TestHandleInstallSnapshotIgnoresStaleRetransmit(t *testing.T) {
	storage := newMemStorage()
	sm := &testStateMachine{}
	n, err := NewNode("node-1", []string{"node-2"}, storage, &fakeTransport{net: newFakeNetwork(), selfID: "node-1"}, sm, testLogger(), testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	firstData, _ := json.Marshal([][]byte{[]byte("a"), []byte("b")})
	reply := n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 1, LeaderID: "node-2", LastIncludedIndex: 5, LastIncludedTerm: 1, Data: firstData})
	if reply.Term != 1 {
		t.Fatalf("first InstallSnapshot reply term = %d, want 1", reply.Term)
	}
	if n.logBaseIndex != 5 {
		t.Fatalf("logBaseIndex after first install = %d, want 5", n.logBaseIndex)
	}

	// A stale retransmit of an OLDER snapshot (index 3 < our current 5)
	// must not regress logBaseIndex or touch the state machine again.
	staleData, _ := json.Marshal([][]byte{[]byte("stale")})
	n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 1, LeaderID: "node-2", LastIncludedIndex: 3, LastIncludedTerm: 1, Data: staleData})

	if n.logBaseIndex != 5 {
		t.Errorf("logBaseIndex after stale retransmit = %d, want unchanged at 5", n.logBaseIndex)
	}
	applied := sm.Applied()
	if len(applied) != 2 || string(applied[0]) != "a" || string(applied[1]) != "b" {
		t.Errorf("state machine after stale retransmit = %v, want unchanged [a b]", applied)
	}
}
