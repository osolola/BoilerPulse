package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingBlockingTransport lets a test control exactly when
// SendAppendEntries returns, and count how many times it was actually
// called -- used to verify that concurrent Propose calls coalesce into a
// bounded number of RPCs per peer instead of one goroutine (and one RPC)
// per proposal.
type countingBlockingTransport struct {
	callCount atomic.Int32
	release   chan struct{}
}

func (t *countingBlockingTransport) SendRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return &RequestVoteReply{Term: args.Term, VoteGranted: false}, nil
}

func (t *countingBlockingTransport) SendAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	t.callCount.Add(1)
	<-t.release
	return &AppendEntriesReply{Term: args.Term, Success: true}, nil
}

// TestConcurrentProposalsCoalesceReplicationPerPeer is a regression test
// for a real instability found via cmd/simulator load testing (see
// docs/benchmarking.md): a burst of concurrent Propose calls used to spawn
// one goroutine (and one AppendEntries RPC) per peer PER proposal, with no
// serialization -- flooding each peer with overlapping, increasingly
// redundant sends that could starve its heartbeats and trigger unnecessary
// elections even though the leader was alive the whole time. Replication
// is now serialized per peer (replication.go's replicationLoop +
// triggerReplication), so a burst should coalesce into only a couple of
// actual RPCs, not one per proposal.
func TestConcurrentProposalsCoalesceReplicationPerPeer(t *testing.T) {
	transport := &countingBlockingTransport{release: make(chan struct{})}
	sm := &testStateMachine{}

	node, err := NewNode("node-1", []string{"peer-1"}, newMemStorage(), transport, sm, testLogger(), testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// Force leadership directly rather than running a real election --
	// this test is specifically about replication fan-out, not election.
	node.mu.Lock()
	node.state = Leader
	node.leaderID = node.id
	node.currentTerm = 1
	node.nextIndex = map[string]uint64{"peer-1": 1}
	node.matchIndex = map[string]uint64{"peer-1": 0}
	node.mu.Unlock()

	node.Start()
	defer node.Stop()

	const proposals = 50
	var wg sync.WaitGroup
	for i := 0; i < proposals; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = node.Propose(ctx, []byte{byte(i)}) // errors ignored: only call-count matters here
		}(i)
	}

	// Give every proposal's triggerReplication call a chance to fire
	// before we release the first blocked RPC -- if replication weren't
	// serialized, callCount would already be climbing toward `proposals`
	// by now.
	time.Sleep(100 * time.Millisecond)
	if got := transport.callCount.Load(); got != 1 {
		t.Errorf("SendAppendEntries call count while first RPC is still in flight = %d, want 1 (all other proposals should have coalesced into the pending trigger, not started their own RPC)", got)
	}

	close(transport.release) // let every blocked/future call return immediately
	wg.Wait()

	// After the first RPC completes, the replication loop should send at
	// most a small, bounded number of follow-up RPCs to pick up whatever
	// coalesced while it was busy -- nowhere near one per proposal.
	if got := transport.callCount.Load(); got < 1 || got > 5 {
		t.Errorf("total SendAppendEntries calls = %d, want roughly 1-5 (coalesced), not up to %d (one per proposal)", got, proposals)
	}
}
