package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// RaftStateMachine adapts an Engine to whatever a Raft implementation
// expects its replicated state machine to look like (an Apply(command
// []byte) error method) — see internal/raft.StateMachine. It's defined
// here rather than in internal/raft so internal/raft never needs to import
// internal/storage; Go's structural interfaces make the adaptation work
// without either package depending on the other.
type RaftStateMachine struct {
	engine Engine
}

// NewRaftStateMachine wraps engine so committed Raft log entries get
// applied to it.
func NewRaftStateMachine(engine Engine) *RaftStateMachine {
	return &RaftStateMachine{engine: engine}
}

// Apply decodes command and applies it to the underlying engine. It's
// called once per committed log entry, in order — including during replay
// of already-applied entries after a restart, so re-applying a Delete for a
// key that's already gone is expected, not an error.
func (sm *RaftStateMachine) Apply(command []byte) error {
	cmd, err := DecodeCommand(command)
	if err != nil {
		return fmt.Errorf("decoding command: %w", err)
	}

	switch cmd.Op {
	case CommandSet:
		return sm.engine.Put(cmd.Key, cmd.Value, cmd.Consistency, time.Duration(cmd.TTLSeconds)*time.Second)
	case CommandDelete:
		err := sm.engine.Delete(cmd.Key)
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("unknown command op %d", cmd.Op)
	}
}

// snapshotEntry is one key's worth of state in a serialized snapshot. Only
// what Put actually accepts is captured -- there's no point preserving the
// engine's internal Version counter, since nothing outside the engine ever
// compares versions across a snapshot boundary, and the restored engine
// assigns its own fresh, internally-consistent versions on replay anyway.
type snapshotEntry struct {
	Key         string      `json:"key"`
	Value       []byte      `json:"value"`
	Consistency Consistency `json:"consistency"`
	ExpiresAt   time.Time   `json:"expires_at,omitempty"`
}

// Snapshot serializes every live (non-tombstoned, non-expired) key in the
// engine -- Scan("") matches everything, since every key has the empty
// string as a prefix. Entries are sorted by key first so two snapshots of
// identical state always encode to identical bytes, which is what makes
// the InstallSnapshot regression test's byte-for-byte comparison possible.
func (sm *RaftStateMachine) Snapshot() ([]byte, error) {
	entries, err := sm.engine.Scan("")
	if err != nil {
		return nil, fmt.Errorf("scanning engine for snapshot: %w", err)
	}

	out := make([]snapshotEntry, 0, len(entries))
	for key, e := range entries {
		out = append(out, snapshotEntry{Key: key, Value: e.Value, Consistency: e.Consistency, ExpiresAt: e.ExpiresAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encoding snapshot: %w", err)
	}
	return data, nil
}

// Restore replaces the engine's state with what data (from a prior
// Snapshot) encodes. Called once at startup when a persisted snapshot is
// loaded, and once per InstallSnapshot RPC received -- in both cases before
// any further log entries are applied, so overwriting existing keys is
// correct rather than merging.
func (sm *RaftStateMachine) Restore(data []byte) error {
	var entries []snapshotEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("decoding snapshot: %w", err)
	}

	for _, e := range entries {
		var ttl time.Duration
		if !e.ExpiresAt.IsZero() {
			ttl = time.Until(e.ExpiresAt)
			if ttl <= 0 {
				continue // expired between snapshotting and restoring; don't resurrect it
			}
		}
		if err := sm.engine.Put(e.Key, e.Value, e.Consistency, ttl); err != nil {
			return fmt.Errorf("restoring key %q: %w", e.Key, err)
		}
	}
	return nil
}
