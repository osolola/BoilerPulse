package sstable

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndOpenRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	entries := []Entry{
		{Key: "a", Value: []byte("1"), Consistency: "EVENTUAL", Version: 1},
		{Key: "b", Value: []byte("2"), Consistency: "STRONG", Version: 2, ExpiresAtUnixNano: 12345},
		{Key: "c", Tombstone: true, Version: 3},
	}
	if err := WriteSorted(path, entries); err != nil {
		t.Fatalf("WriteSorted: %v", err)
	}

	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer table.Close()

	for _, want := range entries {
		got, ok, err := table.Get(want.Key)
		if err != nil {
			t.Fatalf("Get(%q): %v", want.Key, err)
		}
		if !ok {
			t.Fatalf("Get(%q) = not found, want present", want.Key)
		}
		assertEntriesEqual(t, got, want)
	}
}

func TestGetMissingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	if err := WriteSorted(path, []Entry{{Key: "a", Value: []byte("1")}}); err != nil {
		t.Fatalf("WriteSorted: %v", err)
	}
	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer table.Close()

	_, ok, err := table.Get("does-not-exist")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Error("Get(missing) = found, want not found")
	}
}

func TestIterateReturnsAllInSortedOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")
	entries := []Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}
	if err := WriteSorted(path, entries); err != nil {
		t.Fatalf("WriteSorted: %v", err)
	}
	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer table.Close()

	got, err := table.Iterate()
	if err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("Iterate returned %d entries, want %d", len(got), len(entries))
	}
	for i, want := range entries {
		assertEntriesEqual(t, got[i], want)
	}
}

func TestOpenRejectsGarbageFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-table.sst")
	if err := os.WriteFile(path, []byte("not a real sstable"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Error("Open on a garbage file returned nil error, want an error")
	}
}

func TestEmptyTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.sst")
	if err := WriteSorted(path, nil); err != nil {
		t.Fatalf("WriteSorted: %v", err)
	}
	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer table.Close()

	entries, err := table.Iterate()
	if err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Iterate on empty table = %v, want empty", entries)
	}

	_, ok, err := table.Get("anything")
	if err != nil {
		t.Fatalf("Get on empty table: %v", err)
	}
	if ok {
		t.Error("Get on empty table = found, want not found")
	}
}

func assertEntriesEqual(t *testing.T, got, want Entry) {
	t.Helper()
	if got.Key != want.Key || string(got.Value) != string(want.Value) ||
		got.Consistency != want.Consistency || got.Tombstone != want.Tombstone ||
		got.Version != want.Version || got.ExpiresAtUnixNano != want.ExpiresAtUnixNano {
		t.Errorf("entry = %+v, want %+v", got, want)
	}
}
