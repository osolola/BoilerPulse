package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageAppendAndLoadLog(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	want := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	}
	if err := fs.AppendEntries(want); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	assertLogEntriesEqual(t, got, want)
}

func TestFileStorageTermAndVoteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	if err := fs.SaveTermAndVote(5, "node-2"); err != nil {
		t.Fatalf("SaveTermAndVote: %v", err)
	}
	term, votedFor, err := fs.LoadTermAndVote()
	if err != nil {
		t.Fatalf("LoadTermAndVote: %v", err)
	}
	if term != 5 || votedFor != "node-2" {
		t.Errorf("LoadTermAndVote() = (%d, %q), want (5, %q)", term, votedFor, "node-2")
	}
}

func TestFileStorageLoadTermAndVoteBeforeAnySave(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	term, votedFor, err := fs.LoadTermAndVote()
	if err != nil {
		t.Fatalf("LoadTermAndVote: %v", err)
	}
	if term != 0 || votedFor != "" {
		t.Errorf("LoadTermAndVote() on fresh storage = (%d, %q), want (0, \"\")", term, votedFor)
	}
}

func TestFileStorageTruncateFrom(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	if err := fs.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}

	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	want := []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}
	assertLogEntriesEqual(t, got, want)

	// Appending after a truncate must work correctly (offsets stay consistent).
	if err := fs.AppendEntries([]LogEntry{{Term: 2, Index: 2, Command: []byte("new-b")}}); err != nil {
		t.Fatalf("AppendEntries after truncate: %v", err)
	}
	got, err = fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog after append: %v", err)
	}
	want = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 2, Index: 2, Command: []byte("new-b")},
	}
	assertLogEntriesEqual(t, got, want)
}

func TestFileStorageSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}

	if err := fs.SaveTermAndVote(3, "node-1"); err != nil {
		t.Fatalf("SaveTermAndVote: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 3, Index: 2, Command: []byte("b")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage (reopen): %v", err)
	}
	defer reopened.Close()

	term, votedFor, err := reopened.LoadTermAndVote()
	if err != nil {
		t.Fatalf("LoadTermAndVote: %v", err)
	}
	if term != 3 || votedFor != "node-1" {
		t.Errorf("LoadTermAndVote() after restart = (%d, %q), want (3, %q)", term, votedFor, "node-1")
	}

	log, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	want := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 3, Index: 2, Command: []byte("b")},
	}
	assertLogEntriesEqual(t, log, want)
}

func TestFileStorageRecoversFromTornTrailingWrite(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write of a second entry: append a frame claiming
	// more bytes than actually follow before EOF.
	logPath := filepath.Join(dir, "raft-log.bin")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 100, 1, 2, 3}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage after torn write returned error: %v, want a clean recovery", err)
	}
	defer reopened.Close()

	log, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	want := []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}
	assertLogEntriesEqual(t, log, want)

	// The recovered storage must also be writable afterward (the torn tail
	// was actually truncated off disk, not just ignored in memory).
	if err := reopened.AppendEntries([]LogEntry{{Term: 2, Index: 2, Command: []byte("b")}}); err != nil {
		t.Fatalf("AppendEntries after recovery: %v", err)
	}
	log, err = reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	want = append(want, LogEntry{Term: 2, Index: 2, Command: []byte("b")})
	assertLogEntriesEqual(t, log, want)
}

func assertLogEntriesEqual(t *testing.T, got, want []LogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Term != want[i].Term || got[i].Index != want[i].Index || string(got[i].Command) != string(want[i].Command) {
			t.Errorf("entry[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestFileStorageSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	if _, _, _, ok, err := fs.LoadSnapshot(); err != nil {
		t.Fatalf("LoadSnapshot on fresh storage: %v", err)
	} else if ok {
		t.Fatal("LoadSnapshot on fresh storage: ok = true, want false")
	}

	if err := fs.SaveSnapshot(5, 2, []byte("snapshot-payload")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	index, term, data, ok, err := fs.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !ok || index != 5 || term != 2 || string(data) != "snapshot-payload" {
		t.Errorf("LoadSnapshot() = (%d, %d, %q, %v), want (5, 2, %q, true)", index, term, data, ok, "snapshot-payload")
	}
}

func TestFileStorageSnapshotSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	if err := fs.SaveSnapshot(10, 3, []byte("state")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage (reopen): %v", err)
	}
	defer reopened.Close()

	index, term, data, ok, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !ok || index != 10 || term != 3 || string(data) != "state" {
		t.Errorf("LoadSnapshot() after restart = (%d, %d, %q, %v), want (10, 3, %q, true)", index, term, data, ok, "state")
	}
}

func TestFileStorageDiscardLogThroughRemovesPrefix(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	if err := fs.DiscardLogThrough(2); err != nil {
		t.Fatalf("DiscardLogThrough: %v", err)
	}

	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	want := []LogEntry{
		{Term: 2, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}
	assertLogEntriesEqual(t, got, want)

	// Appending and truncating after a prefix discard must still use the
	// right positions (baseIndex bookkeeping), not the pre-discard ones.
	if err := fs.AppendEntries([]LogEntry{{Term: 2, Index: 5, Command: []byte("e")}}); err != nil {
		t.Fatalf("AppendEntries after discard: %v", err)
	}
	if err := fs.TruncateFrom(4); err != nil {
		t.Fatalf("TruncateFrom after discard: %v", err)
	}
	got, err = fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	want = []LogEntry{{Term: 2, Index: 3, Command: []byte("c")}}
	assertLogEntriesEqual(t, got, want)
}

func TestFileStorageDiscardLogThroughSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := fs.SaveSnapshot(2, 1, []byte("snap")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := fs.DiscardLogThrough(2); err != nil {
		t.Fatalf("DiscardLogThrough: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A fresh OpenFileStorage must reconstruct baseIndex from the snapshot
	// on disk -- LoadLog should return only what's actually left on disk
	// (the post-compaction tail), and appending afterward must still land
	// at the right file offsets.
	reopened, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage (reopen): %v", err)
	}
	defer reopened.Close()

	got, err := reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	assertLogEntriesEqual(t, got, []LogEntry{{Term: 1, Index: 3, Command: []byte("c")}})

	if err := reopened.AppendEntries([]LogEntry{{Term: 1, Index: 4, Command: []byte("d")}}); err != nil {
		t.Fatalf("AppendEntries after reopen: %v", err)
	}
	got, err = reopened.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	assertLogEntriesEqual(t, got, []LogEntry{
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 1, Index: 4, Command: []byte("d")},
	})
}

func TestFileStorageDiscardLogThroughEverything(t *testing.T) {
	// A follower installing a snapshot that covers its entire local log
	// (or more) must end up with an empty log, not an error or a panic.
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	defer fs.Close()

	if err := fs.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := fs.DiscardLogThrough(100); err != nil {
		t.Fatalf("DiscardLogThrough(100) on a 2-entry log: %v", err)
	}
	got, err := fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("LoadLog() after discarding past the end = %+v, want empty", got)
	}

	if err := fs.AppendEntries([]LogEntry{{Term: 2, Index: 101, Command: []byte("fresh")}}); err != nil {
		t.Fatalf("AppendEntries after full discard: %v", err)
	}
	got, err = fs.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	assertLogEntriesEqual(t, got, []LogEntry{{Term: 2, Index: 101, Command: []byte("fresh")}})
}

func TestOpenFileStorageCleansUpOrphanedTempFiles(t *testing.T) {
	// Simulates a crash between writing a .tmp file and renaming it into
	// place (DiscardLogThrough / SaveSnapshot / SaveTermAndVote all use
	// this protocol) -- the real file is untouched by construction, so the
	// only thing to clean up is the leftover .tmp litter.
	dir := t.TempDir()
	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, name := range []string{"raft-log.bin.tmp", "raft-snapshot.bin.tmp", "raft-state.json.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("garbage"), 0o644); err != nil {
			t.Fatalf("writing orphaned %s: %v", name, err)
		}
	}

	reopened, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage with orphaned temp files present: %v", err)
	}
	defer reopened.Close()

	for _, name := range []string{"raft-log.bin.tmp", "raft-snapshot.bin.tmp", "raft-state.json.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s still exists after OpenFileStorage, want it cleaned up", name)
		}
	}
}
