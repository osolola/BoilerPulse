package storage

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCommandEncodeDecodeRoundTrip(t *testing.T) {
	cmd := Command{Op: CommandSet, Key: "k", Value: []byte(`{"a":1}`), Consistency: ConsistencyStrong, TTLSeconds: 60}

	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	got, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.Op != cmd.Op || got.Key != cmd.Key || string(got.Value) != string(cmd.Value) ||
		got.Consistency != cmd.Consistency || got.TTLSeconds != cmd.TTLSeconds {
		t.Errorf("round trip = %+v, want %+v", got, cmd)
	}
}

func TestRaftStateMachineApplySet(t *testing.T) {
	engine := NewMemStore()
	sm := NewRaftStateMachine(engine)

	cmd, _ := EncodeCommand(Command{Op: CommandSet, Key: "k", Value: []byte("v"), Consistency: ConsistencyEventual})
	if err := sm.Apply(cmd); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	entry, err := engine.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(entry.Value) != "v" {
		t.Errorf("Value = %q, want %q", entry.Value, "v")
	}
}

func TestRaftStateMachineApplyDelete(t *testing.T) {
	engine := NewMemStore()
	sm := NewRaftStateMachine(engine)

	setCmd, _ := EncodeCommand(Command{Op: CommandSet, Key: "k", Value: []byte("v")})
	if err := sm.Apply(setCmd); err != nil {
		t.Fatalf("Apply(set): %v", err)
	}

	delCmd, _ := EncodeCommand(Command{Op: CommandDelete, Key: "k"})
	if err := sm.Apply(delCmd); err != nil {
		t.Fatalf("Apply(delete): %v", err)
	}

	if _, err := engine.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after delete = %v, want ErrKeyNotFound", err)
	}
}

func TestRaftStateMachineApplyDeleteOfAlreadyMissingKeyIsNotAnError(t *testing.T) {
	// Re-applying a Delete for a key that's already gone happens naturally
	// during WAL/Raft-log replay after a restart -- it must not error.
	engine := NewMemStore()
	sm := NewRaftStateMachine(engine)

	delCmd, _ := EncodeCommand(Command{Op: CommandDelete, Key: "never-existed"})
	if err := sm.Apply(delCmd); err != nil {
		t.Errorf("Apply(delete of missing key) = %v, want nil", err)
	}
}

func TestRaftStateMachineApplyUnknownOp(t *testing.T) {
	engine := NewMemStore()
	sm := NewRaftStateMachine(engine)

	cmd, _ := EncodeCommand(Command{Op: 99, Key: "k"})
	if err := sm.Apply(cmd); err == nil {
		t.Error("Apply with unknown op returned nil error, want an error")
	}
}

func TestRaftStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	source := NewMemStore()
	sm := NewRaftStateMachine(source)

	if err := source.Put("a", []byte("1"), ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := source.Put("b", []byte("2"), ConsistencyStrong, 0); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := source.Put("c", []byte("3"), ConsistencyCritical, time.Hour); err != nil {
		t.Fatalf("Put c: %v", err)
	}

	data, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a completely separate, empty engine -- this is exactly
	// what happens when a follower installs a leader's snapshot.
	target := NewMemStore()
	targetSM := NewRaftStateMachine(target)
	if err := targetSM.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for _, tc := range []struct {
		key         string
		wantValue   string
		wantConsist Consistency
	}{
		{"a", "1", ConsistencyEventual},
		{"b", "2", ConsistencyStrong},
		{"c", "3", ConsistencyCritical},
	} {
		entry, err := target.Get(tc.key)
		if err != nil {
			t.Errorf("Get(%q) after restore: %v", tc.key, err)
			continue
		}
		if string(entry.Value) != tc.wantValue || entry.Consistency != tc.wantConsist {
			t.Errorf("restored %q = (%s, %s), want (%s, %s)", tc.key, entry.Value, entry.Consistency, tc.wantValue, tc.wantConsist)
		}
	}
}

func TestRaftStateMachineSnapshotIsDeterministic(t *testing.T) {
	// Two snapshots of identical state must encode to identical bytes --
	// map iteration order is randomized in Go, so this only holds because
	// Snapshot sorts by key before encoding.
	engine := NewMemStore()
	sm := NewRaftStateMachine(engine)
	for _, k := range []string{"z", "a", "m", "b", "y"} {
		if err := engine.Put(k, []byte(k), ConsistencyEventual, 0); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	first, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot (first): %v", err)
	}
	second, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot (second): %v", err)
	}
	if string(first) != string(second) {
		t.Errorf("two snapshots of the same unchanged state produced different bytes:\nfirst:  %s\nsecond: %s", first, second)
	}
}

func TestRaftStateMachineRestoreSkipsExpiredEntries(t *testing.T) {
	// Both entries are encoded directly (not via Put+sleep), so the
	// already-expired one is deterministic and the test stays fast.
	entries := []snapshotEntry{
		{Key: "live", Value: []byte("v"), Consistency: ConsistencyEventual, ExpiresAt: time.Now().Add(time.Hour)},
		{Key: "already-expired", Value: []byte("v"), Consistency: ConsistencyEventual, ExpiresAt: time.Now().Add(-time.Minute)},
	}
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	target := NewMemStore()
	targetSM := NewRaftStateMachine(target)
	if err := targetSM.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, err := target.Get("live"); err != nil {
		t.Errorf("Get(live) after restore: %v, want it present", err)
	}
	if _, err := target.Get("already-expired"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get(already-expired) after restore = %v, want ErrKeyNotFound (should not have been resurrected)", err)
	}
}
