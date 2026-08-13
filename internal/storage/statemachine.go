package storage

import (
	"errors"
	"fmt"
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
