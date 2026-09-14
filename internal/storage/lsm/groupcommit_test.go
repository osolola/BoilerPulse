package lsm

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"boilerpulse/internal/storage"
	"boilerpulse/internal/storage/wal"
)

// countingBlockingWAL lets a test control exactly when AppendBatch
// returns, and count how many times it was actually called and how many
// records it saw in total -- used to verify that concurrent Put/Delete
// calls coalesce into a bounded number of fsyncs instead of one per call.
// Mirrors internal/raft/replication_test.go's countingBlockingTransport,
// same pattern applied one layer down.
type countingBlockingWAL struct {
	callCount   atomic.Int32
	recordCount atomic.Int32
	release     chan struct{}
}

func (w *countingBlockingWAL) AppendBatch(records []wal.Record) error {
	w.callCount.Add(1)
	w.recordCount.Add(int32(len(records)))
	<-w.release
	return nil
}

func (w *countingBlockingWAL) Reset() error { return nil }
func (w *countingBlockingWAL) Close() error { return nil }

// newTestEngineWithWAL builds a minimal, ready-to-use Engine around a
// caller-supplied walWriter fake, without going through Open (which always
// constructs a real *wal.Writer on disk) -- an in-package test can do this
// directly since engineOp/opLoop/etc. are unexported.
func newTestEngineWithWAL(w walWriter) *Engine {
	e := &Engine{
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		opts:     DefaultOptions(),
		wal:      w,
		memtable: make(map[string]storage.Entry),
		nextSeq:  1,
		opCh:     make(chan engineOp, 1024),
		stopCh:   make(chan struct{}),
		opDoneCh: make(chan struct{}),
	}
	go e.opLoop()
	return e
}

// TestConcurrentPutsCoalesceIntoFewerWALBatches is a regression test for
// the write-throughput ceiling documented in docs/benchmarking.md: Put
// used to hold e.mu for the full duration of its own synchronous WAL
// fsync, so N concurrent Put calls always meant N separate fsyncs, fully
// serialized (the (i+1)th couldn't even start until the ith's fsync had
// completely finished and released the lock). With group commit, several
// concurrent Puts queued up behind one in-flight fsync all land in the
// NEXT batch together.
func TestConcurrentPutsCoalesceIntoFewerWALBatches(t *testing.T) {
	fakeWAL := &countingBlockingWAL{release: make(chan struct{})}
	e := newTestEngineWithWAL(fakeWAL)
	defer e.stop()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := e.Put(fmt.Sprintf("k%d", i), []byte("v"), storage.ConsistencyEventual, 0); err != nil {
				t.Errorf("Put(%d): %v", i, err)
			}
		}(i)
	}

	// Give every goroutine a chance to actually reach the point of being
	// blocked on the WAL (either inside the first in-flight AppendBatch
	// call, or queued on opCh waiting for it) before releasing anything.
	waitForCondition(t, time.Second, "all 50 Puts to be in flight", func() bool {
		return fakeWAL.callCount.Load() >= 1
	})
	time.Sleep(50 * time.Millisecond) // let stragglers pile up on opCh behind the first in-flight call

	close(fakeWAL.release) // let every currently-blocked AppendBatch call return at once
	wg.Wait()

	calls := fakeWAL.callCount.Load()
	records := fakeWAL.recordCount.Load()
	if records != n {
		t.Fatalf("total records seen across all AppendBatch calls = %d, want %d", records, n)
	}
	if calls >= n {
		t.Errorf("AppendBatch was called %d times for %d concurrent Puts, want meaningfully fewer (batching should have coalesced them)", calls, n)
	}
	t.Logf("%d concurrent Puts coalesced into %d AppendBatch call(s)", n, calls)
}

// TestConcurrentPutsToSameKeyApplyInSeqOrder guards the subtle correctness
// requirement group commit introduces: Put assigns a seq number and
// enqueues while holding e.mu (fixing its position in WAL/seq order), then
// releases the lock and waits -- so a LATER Put (to the SAME key) that
// happens to reach the front of the queue in a DIFFERENT batch, or even
// the same batch, must still end up applied to the memtable strictly after
// an earlier one, never the reverse. Getting this wrong would be a lost
// update: two concurrent writers to one key, and the "older" one silently
// winning.
func TestConcurrentPutsToSameKeyApplyInSeqOrder(t *testing.T) {
	dataDir := t.TempDir()
	e, err := Open(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)), DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// The engine assigns seq under e.mu at the moment this call
			// reaches Put, so whichever call gets seq N is, by
			// construction, the Nth to be applied -- this test only
			// needs to confirm the FINAL memtable state matches whichever
			// seq actually landed last, not any particular interleaving.
			if err := e.Put("hot-key", []byte(fmt.Sprintf("v%d", i)), storage.ConsistencyEventual, 0); err != nil {
				t.Errorf("Put(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	entry, err := e.Get("hot-key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Whatever value ended up stored, its Version (the seq it was
	// assigned) must be the highest seq handed out -- i.e. the memtable
	// reflects the LAST writer in seq order, not whichever goroutine
	// happened to win a scheduling race after its fsync returned.
	if entry.Version != n {
		t.Errorf("final Version = %d, want %d (the highest seq assigned) -- a lower version winning means a concurrent write applied out of seq order", entry.Version, n)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for: %s", msg)
	}
}
