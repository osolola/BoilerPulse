package failure

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// This file's four tests are spec §48's failure scenarios, run against a
// real 3-node cluster (see harness_test.go): real disk, real Raft, real
// gRPC, real HTTP. Nothing here is simulated at the protocol level --
// "kill" means the process's components are actually torn down, and
// "restart" means they're actually reopened from the same directories.

// TestKillFollowerSystemRemainsAvailable: killing one follower must not
// interrupt writes or reads -- the leader still has a majority (2 of 3)
// without it.
func TestKillFollowerSystemRemainsAvailable(t *testing.T) {
	members := newFailureCluster(t, 3)

	leader := waitForLeader(members, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 5s")
	}

	var follower *member
	for _, m := range members {
		if m != leader {
			follower = m
			break
		}
	}
	follower.kill(t)

	resp := put(t, leader, "still-here", `{"value":"yes"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT to leader after killing a follower: status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	var survivor *member
	for _, m := range members {
		if m != leader && m != follower {
			survivor = m
			break
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	var lastStatus int
	for time.Now().Before(deadline) {
		resp := get(t, survivor, "still-here")
		lastStatus = resp.StatusCode
		resp.Body.Close()
		if lastStatus == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastStatus != http.StatusOK {
		t.Fatalf("surviving follower never saw the replicated write; last GET status = %d", lastStatus)
	}
}

// TestKillLeaderElectsNewLeader: killing the leader must not stop the
// cluster from accepting writes -- the remaining 2 of 3 nodes still form a
// majority and elect a successor.
func TestKillLeaderElectsNewLeader(t *testing.T) {
	members := newFailureCluster(t, 3)

	leader := waitForLeader(members, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 5s")
	}
	firstTerm := leader.term()
	leader.kill(t)

	newLeader := waitForNewLeader(members, leader, firstTerm, 5*time.Second)
	if newLeader == nil {
		t.Fatal("no new leader elected after the original leader was killed")
	}

	resp := put(t, newLeader, "after-failover", `{"value":"still-works"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT to new leader: status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

// TestKillTwoOfThreeLosesQuorumWritesRejected: killing the leader plus one
// follower leaves a single survivor, which can never win a majority (it
// needs 2 of 3 votes and only has its own) -- writes must be rejected, and
// critically the survivor must never believe itself leader (no split
// brain: at most one leader per term is Raft's Election Safety property,
// and here there must be no leader at all).
func TestKillTwoOfThreeLosesQuorumWritesRejected(t *testing.T) {
	members := newFailureCluster(t, 3)

	leader := waitForLeader(members, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 5s")
	}

	var toKillAlso, survivor *member
	for _, m := range members {
		if m == leader {
			continue
		}
		if toKillAlso == nil {
			toKillAlso = m
		} else {
			survivor = m
		}
	}

	leader.kill(t)
	toKillAlso.kill(t)

	// Give the survivor several election-timeout cycles to (fail to) find
	// a majority, then confirm it never claims leadership.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if survivor.isLeader() {
			t.Fatal("lone survivor (1 of 3, no quorum) claims to be leader -- split brain")
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp := put(t, survivor, "should-be-rejected", `{"value":"x"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("PUT to lone survivor with no quorum: status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err == nil && body.Error.Code != "LEADER_UNAVAILABLE" {
		t.Errorf("error code = %q, want %q", body.Error.Code, "LEADER_UNAVAILABLE")
	}
}

// TestRestartOldLeaderRejoinsWithoutOverwritingCommittedData: the old
// leader, once restarted, must rejoin as a follower, catch up on
// everything committed while it was down, and must not clobber that data
// with anything it (uncommitted) had proposed to itself before dying.
func TestRestartOldLeaderRejoinsWithoutOverwritingCommittedData(t *testing.T) {
	members := newFailureCluster(t, 3)

	leader := waitForLeader(members, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 5s")
	}
	firstTerm := leader.term()

	resp := put(t, leader, "before-crash", `{"value":"original"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT before crash: status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	oldLeader := leader
	oldLeader.kill(t)

	newLeader := waitForNewLeader(members, oldLeader, firstTerm, 5*time.Second)
	if newLeader == nil {
		t.Fatal("no new leader elected after the original leader was killed")
	}

	resp = put(t, newLeader, "after-crash", `{"value":"committed-while-down"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT to new leader while old leader is down: status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	oldLeader.start(t)

	// The restarted node must catch up on the write that happened while it
	// was down, and must still see its own pre-crash write (both
	// committed, so both must survive).
	deadline := time.Now().Add(5 * time.Second)
	var sawAfterCrash, sawBeforeCrash bool
	for time.Now().Before(deadline) && !(sawAfterCrash && sawBeforeCrash) {
		r1 := get(t, oldLeader, "after-crash")
		if r1.StatusCode == http.StatusOK {
			var body struct {
				Value json.RawMessage `json:"value"`
			}
			json.NewDecoder(r1.Body).Decode(&body)
			if string(body.Value) == `"committed-while-down"` {
				sawAfterCrash = true
			}
		}
		r1.Body.Close()

		r2 := get(t, oldLeader, "before-crash")
		if r2.StatusCode == http.StatusOK {
			var body struct {
				Value json.RawMessage `json:"value"`
			}
			json.NewDecoder(r2.Body).Decode(&body)
			if string(body.Value) == `"original"` {
				sawBeforeCrash = true
			}
		}
		r2.Body.Close()

		if !(sawAfterCrash && sawBeforeCrash) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !sawAfterCrash {
		t.Error("restarted old leader never caught up on the write committed while it was down")
	}
	if !sawBeforeCrash {
		t.Error("restarted old leader lost its own pre-crash committed write")
	}

	// It must not still believe it's leader from its old term -- it has to
	// have rejoined as a follower (or at most re-won an election in a
	// strictly higher term than the one it died in; either way, exactly
	// one leader cluster-wide).
	leaders := 0
	for _, m := range members {
		if m.isLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("cluster has %d leaders after old leader rejoined, want exactly 1", leaders)
	}
}
