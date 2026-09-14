package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingBlockingStorage lets a test control exactly when AppendEntries
// returns, and count how many times it was actually called and how many
// entries it saw in total -- verifies that concurrent Propose calls
// coalesce into a bounded number of persist calls (and so, on FileStorage,
// fsyncs) instead of one per proposal. Same pattern as
// countingBlockingWAL (internal/storage/lsm) and countingBlockingTransport
// (replication_test.go) applied to the third leg of this project's
// group-commit work.
type countingBlockingStorage struct {
	memStorage
	callCount  atomic.Int32
	entryCount atomic.Int32
	release    chan struct{}
}

func (s *countingBlockingStorage) AppendEntries(entries []LogEntry) error {
	s.callCount.Add(1)
	s.entryCount.Add(int32(len(entries)))
	<-s.release
	return s.memStorage.AppendEntries(entries)
}

// TestConcurrentProposalsCoalesceIntoFewerAppendCalls is a regression test
// for the write-throughput ceiling documented in docs/benchmarking.md:
// Propose used to hold n.mu for the full duration of its own synchronous
// append-and-fsync (via appendLogLocked -> storage.AppendEntries), so N
// concurrent Propose calls always meant N separate persist calls, fully
// serialized. With group commit (appendLoop, apply.go), several proposals
// queued up behind one in-flight append all land in the NEXT batch
// together -- one persist call, and on the real FileStorage, one fsync,
// covering all of them.
func TestConcurrentProposalsCoalesceIntoFewerAppendCalls(t *testing.T) {
	storage := &countingBlockingStorage{release: make(chan struct{})}
	sm := &testStateMachine{}
	n, err := NewNode("node-1", nil, storage, &fakeTransport{net: newFakeNetwork(), selfID: "node-1"}, sm, testLogger(), testOptions())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.mu.Lock()
	n.state = Leader // no peers to elect against; force leadership directly
	n.mu.Unlock()
	n.Start()
	defer n.Stop()

	const proposals = 50
	var wg sync.WaitGroup
	for i := 0; i < proposals; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			// waitForApply will time out (nothing ever advances
			// commitIndex with zero peers and a leader that never
			// heartbeats itself) -- this test only cares about the
			// append/persist phase, not full commitment, so a context
			// deadline here is an expected, harmless way for Propose to
			// return once its entry is safely appended.
			_ = n.Propose(ctx, []byte("entry"))
		}()
	}

	waitFor(t, time.Second, "all proposals to be in flight", func() bool {
		return storage.callCount.Load() >= 1
	})
	time.Sleep(50 * time.Millisecond) // let stragglers pile up on proposeCh behind the first in-flight call

	close(storage.release)
	wg.Wait()

	calls := storage.callCount.Load()
	entries := storage.entryCount.Load()
	if entries != proposals {
		t.Fatalf("total entries seen across all AppendEntries calls = %d, want %d", entries, proposals)
	}
	if calls >= proposals {
		t.Errorf("AppendEntries was called %d times for %d concurrent proposals, want meaningfully fewer (batching should have coalesced them)", calls, proposals)
	}
	t.Logf("%d concurrent proposals coalesced into %d AppendEntries call(s)", proposals, calls)
}
