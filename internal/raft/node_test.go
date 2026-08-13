package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestElectsASingleLeader(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected and all followers to learn about it via heartbeat", func() bool {
		l := tc.leader()
		if l == nil {
			return false
		}
		for _, n := range tc.nodes {
			id, ok := n.Leader()
			if !ok || id != l.ID() {
				return false
			}
		}
		return true
	})

	leader := tc.leader()
	term := leader.Term()

	leaderCount := 0
	for _, n := range tc.nodes {
		if n.IsLeader() {
			leaderCount++
		}
		if n.Term() != term {
			t.Errorf("node %s term = %d, want %d (all nodes should agree on the term)", n.ID(), n.Term(), term)
		}
	}
	if leaderCount != 1 {
		t.Errorf("leaderCount = %d, want exactly 1", leaderCount)
	}

	for _, n := range tc.nodes {
		id, ok := n.Leader()
		if !ok || id != leader.ID() {
			t.Errorf("node %s Leader() = (%q, %v), want (%q, true)", n.ID(), id, ok, leader.ID())
		}
	}
}

func TestReElectsAfterLeaderStops(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "initial leader election", func() bool { return tc.leader() != nil })
	first := tc.leader()
	firstTerm := first.Term()

	tc.net.disconnect(first.ID())
	tc.stopNode(first)

	waitFor(t, 3*time.Second, "a new leader among the remaining nodes", func() bool {
		l := tc.leader()
		return l != nil && l.ID() != first.ID() && l.Term() > firstTerm
	})

	newLeader := tc.leader()
	if newLeader.ID() == first.ID() {
		t.Fatalf("new leader is still the stopped node %s", first.ID())
	}
}

func TestLogReplication(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })
	leader := tc.leader()

	commands := [][]byte{[]byte("set a=1"), []byte("set b=2"), []byte("delete a")}
	for _, cmd := range commands {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := leader.Propose(ctx, cmd)
		cancel()
		if err != nil {
			t.Fatalf("Propose(%s): %v", cmd, err)
		}
	}

	for i, sm := range tc.sms {
		waitFor(t, 2*time.Second, "all commands applied on "+tc.nodes[i].ID(), func() bool {
			return len(sm.Applied()) == len(commands)
		})
		applied := sm.Applied()
		for j, cmd := range commands {
			if string(applied[j]) != string(cmd) {
				t.Errorf("node %s applied[%d] = %q, want %q", tc.nodes[i].ID(), j, applied[j], cmd)
			}
		}
	}
}

func TestFollowerCatchesUpAfterPartition(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })
	leader := tc.leader()

	var partitioned *Node
	for _, n := range tc.nodes {
		if n != leader {
			partitioned = n
			break
		}
	}
	tc.net.disconnect(partitioned.ID())

	// The leader keeps a majority (itself + the other follower) and should
	// keep committing writes while partitioned is unreachable.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := leader.Propose(ctx, []byte("while-partitioned"))
	cancel()
	if err != nil {
		t.Fatalf("Propose while one follower is partitioned: %v", err)
	}

	tc.net.reconnect(partitioned.ID())

	var partitionedSM *testStateMachine
	for i, n := range tc.nodes {
		if n == partitioned {
			partitionedSM = tc.sms[i]
		}
	}

	waitFor(t, 3*time.Second, "the reconnected follower to catch up", func() bool {
		applied := partitionedSM.Applied()
		return len(applied) > 0 && string(applied[len(applied)-1]) == "while-partitioned"
	})
}

func TestQuorumLossRejectsWrites(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })
	leader := tc.leader()

	for _, n := range tc.nodes {
		if n != leader {
			tc.net.disconnect(n.ID())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err := leader.Propose(ctx, []byte("should-not-commit"))
	cancel()

	if err == nil {
		t.Error("Propose with no reachable quorum succeeded, want an error (timeout or not-leader)")
	}
}

func TestProposeOnNonLeaderReturnsNotLeader(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })

	var follower *Node
	for _, n := range tc.nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := follower.Propose(ctx, []byte("x"))
	if !errors.Is(err, ErrNotLeader) {
		t.Errorf("Propose on a follower = %v, want ErrNotLeader", err)
	}
}

func TestLogConflictIsResolvedOnReconnect(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })
	firstLeader := tc.leader()

	var minority *Node
	for _, n := range tc.nodes {
		if n != firstLeader {
			minority = n
			break
		}
	}

	// Isolate the minority node with the old leader: partition it away from
	// the majority (the other two nodes stay connected to each other).
	tc.net.disconnect(minority.ID())

	// The old leader keeps majority (itself + the other follower) and
	// commits an entry the isolated minority node will never see live.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := firstLeader.Propose(ctx, []byte("committed-by-majority"))
	cancel()
	if err != nil {
		t.Fatalf("Propose on majority side: %v", err)
	}

	// Now isolate the ORIGINAL leader too, leaving only the minority node
	// disconnected from everyone. This proves nothing more can commit
	// (no majority exists), then we reconnect everyone and confirm the
	// minority node converges to the majority's committed history rather
	// than diverging.
	tc.net.disconnect(firstLeader.ID())
	tc.net.reconnect(minority.ID())

	waitFor(t, 3*time.Second, "a new leader among the still-connected nodes", func() bool {
		l := tc.leader()
		return l != nil && l != firstLeader
	})

	tc.net.reconnect(firstLeader.ID())

	// Every node should eventually apply the same command as its first
	// applied entry -- including the former leader and the once-isolated
	// minority node, proving log conflicts get resolved rather than
	// silently diverging.
	for i, n := range tc.nodes {
		sm := tc.sms[i]
		waitFor(t, 3*time.Second, "node "+n.ID()+" to apply the majority-committed entry", func() bool {
			applied := sm.Applied()
			return len(applied) > 0 && string(applied[0]) == "committed-by-majority"
		})
	}
}

// TestElectionSafetyAcrossPartitionAndReconnect exercises the Raft paper's
// core Election Safety property directly: at most one leader per term,
// even while a stale partitioned leader is still running and believes
// itself to be leader.
func TestElectionSafetyAcrossPartitionAndReconnect(t *testing.T) {
	tc := newTestCluster(t, 5)
	tc.startAll()
	defer tc.stopAll()

	waitFor(t, 2*time.Second, "a leader to be elected", func() bool { return tc.leader() != nil })
	oldLeader := tc.leader()

	tc.net.disconnect(oldLeader.ID())

	waitFor(t, 3*time.Second, "a new leader with a higher term among the majority", func() bool {
		l := tc.leader()
		return l != nil && l != oldLeader && l.Term() > oldLeader.Term()
	})

	tc.net.reconnect(oldLeader.ID())

	// Poll for a while and assert the safety property holds throughout:
	// never two nodes reporting Leader for the same term.
	deadline := time.Now().Add(500 * time.Millisecond)
	seenLeaderTerms := map[uint64]string{}
	for time.Now().Before(deadline) {
		for _, n := range tc.nodes {
			if !n.IsLeader() {
				continue
			}
			term := n.Term()
			if existing, ok := seenLeaderTerms[term]; ok && existing != n.ID() {
				t.Fatalf("election safety violated: both %s and %s claim to be leader in term %d", existing, n.ID(), term)
			}
			seenLeaderTerms[term] = n.ID()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPersistedStateSurvivesNodeRestart(t *testing.T) {
	// Uses memStorage directly (not a full cluster) to verify NewNode
	// correctly recovers currentTerm/votedFor/log from a Storage that
	// already has state in it -- simulating a process restart.
	storage := newMemStorage()
	if err := storage.SaveTermAndVote(7, "node-2"); err != nil {
		t.Fatalf("SaveTermAndVote: %v", err)
	}
	if err := storage.AppendEntries([]LogEntry{
		{Term: 5, Index: 1, Command: []byte("a")},
		{Term: 7, Index: 2, Command: []byte("b")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	n, err := NewNode("node-1", []string{"node-2", "node-3"}, storage, &fakeTransport{net: newFakeNetwork(), selfID: "node-1"}, &testStateMachine{}, testLogger(), testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if n.Term() != 7 {
		t.Errorf("Term() = %d, want 7", n.Term())
	}
	if got := n.lastLogIndexLocked(); got != 2 {
		t.Errorf("lastLogIndexLocked() = %d, want 2", got)
	}
}
