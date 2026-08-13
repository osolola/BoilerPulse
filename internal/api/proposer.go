package api

import (
	"context"

	"boilerpulse/internal/storage"
)

// RaftStatus is a snapshot of this node's Raft state, for the /v1/cluster
// endpoint. Plain strings/bools rather than internal/raft types, so this
// package doesn't need to import internal/raft — cmd/node supplies an
// adapter that implements Proposer on top of a *raft.Node.
type RaftStatus struct {
	State    string // "FOLLOWER" | "CANDIDATE" | "LEADER"
	Term     uint64
	LeaderID string
	IsLeader bool
}

// Proposer routes a write through consensus before it's visible anywhere.
// When Server has no Proposer set, PUT/DELETE fall back to writing directly
// to the storage engine — the Milestone 1/2 behavior, still used by tests
// and by any single-node configuration without Raft.
type Proposer interface {
	Propose(ctx context.Context, cmd storage.Command) error
	Status() RaftStatus
}
