package storage

import (
	"errors"
	"testing"
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
