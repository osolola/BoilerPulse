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
