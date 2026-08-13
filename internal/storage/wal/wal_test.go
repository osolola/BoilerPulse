package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	rec := Record{
		Seq:               42,
		Op:                OpSet,
		Timestamp:         1_000_000,
		ExpiresAtUnixNano: 2_000_000,
		Key:               "event:123",
		Consistency:       "EVENTUAL",
		Value:             []byte(`{"title":"Purdue Basketball"}`),
	}

	got, err := decode(bytes.NewReader(encode(rec)))
	if err != nil {
		t.Fatalf("decode returned error: %v", err)
	}
	assertRecordsEqual(t, got, rec)
}

func TestEncodeDecodeDeleteRoundTrip(t *testing.T) {
	rec := Record{Seq: 1, Op: OpDelete, Timestamp: 500, Key: "k"}

	got, err := decode(bytes.NewReader(encode(rec)))
	if err != nil {
		t.Fatalf("decode returned error: %v", err)
	}
	assertRecordsEqual(t, got, rec)
}

func TestWriterAppendThenReadAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	want := []Record{
		{Seq: 1, Op: OpSet, Key: "a", Value: []byte("1"), Consistency: "EVENTUAL"},
		{Seq: 2, Op: OpSet, Key: "b", Value: []byte("2"), Consistency: "STRONG"},
		{Seq: 3, Op: OpDelete, Key: "a"},
	}
	for _, rec := range want {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("ReadAll returned %d records, want %d", len(got), len(want))
	}
	for i := range want {
		assertRecordsEqual(t, got[i], want[i])
	}
}

func TestReadAllMissingFile(t *testing.T) {
	got, err := ReadAll(filepath.Join(t.TempDir(), "does-not-exist.log"))
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ReadAll = %v, want empty", got)
	}
}

func TestReadAllStopsAtTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := w.Append(Record{Seq: 1, Op: OpSet, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write of a second record: a length prefix that
	// promises more bytes than actually follow before EOF.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 100, 1, 2, 3}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v, want nil (a torn tail must not fail replay)", err)
	}
	if len(got) != 1 {
		t.Fatalf("ReadAll returned %d records, want 1 (the torn record should be dropped)", len(got))
	}
}

func TestReadAllStopsAtCorruptChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := w.Append(Record{Seq: 1, Op: OpSet, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(Record{Seq: 2, Op: OpSet, Key: "b", Value: []byte("2")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-5] ^= 0xFF // flip a byte inside the second record's payload
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("ReadAll returned %d records, want 1 (corrupt record should be dropped)", len(got))
	}
}

func TestWriterReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := w.Append(Record{Seq: 1, Op: OpSet, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := w.Append(Record{Seq: 2, Op: OpSet, Key: "b", Value: []byte("2")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("ReadAll after Reset = %+v, want only the post-reset record", got)
	}
}

func assertRecordsEqual(t *testing.T, got, want Record) {
	t.Helper()
	if got.Seq != want.Seq || got.Op != want.Op || got.Timestamp != want.Timestamp ||
		got.ExpiresAtUnixNano != want.ExpiresAtUnixNano || got.Key != want.Key ||
		got.Consistency != want.Consistency || !bytes.Equal(got.Value, want.Value) {
		t.Errorf("record = %+v, want %+v", got, want)
	}
}
