# Raft

`internal/raft` is a hand-rolled implementation of the Raft consensus
algorithm (Ongaro & Ousterhout, "In Search of an Understandable Consensus
Algorithm") — leader election and log replication, no snapshotting/log
compaction yet (explicitly out of scope for this milestone).

## Design: transport- and storage-agnostic core

`raft.Node` depends only on three interfaces:

```
Transport     -- sends RequestVote/AppendEntries RPCs to a named peer
Storage       -- persists currentTerm, votedFor, and the log
StateMachine  -- Apply(command []byte) error, called once per committed entry
```

This is deliberately the same shape etcd's raft library uses, and for the
same reason: it lets the algorithm itself be tested fast and deterministically
with a fake in-memory network (`internal/raft/testutil_test.go`), completely
separately from whether the real gRPC transport or file-based persistence
work correctly. Three implementations exist:

- **Fake transport + in-memory storage** (`internal/raft/testutil_test.go`,
  test-only): synchronous in-process RPC delivery, with the ability to
  simulate a node being disconnected. This is what `node_test.go`'s 9 tests
  run against — elections, failover, partitions, log conflicts all resolve
  in well under a second per test.
- **Real gRPC transport** (`internal/raft/rpc`, `pkg/protocol/raft.proto`):
  what `cmd/node` actually uses. Verified against real TCP sockets in
  `internal/raft/rpc/rpc_test.go`.
- **File-backed storage** (`internal/raft/filestorage.go`): what `cmd/node`
  actually uses. Verified in `internal/raft/filestorage_test.go`, including
  crash-recovery from a torn trailing write, mirroring
  `internal/storage/wal`'s approach.

## Why gRPC/protobuf for internal RPC, not the KV API's HTTP/JSON

`pkg/protocol/raft.proto` defines `RaftService` (RequestVote, AppendEntries).
This is a different wire format from the client-facing KV API
(`internal/api`, plain HTTP/JSON) on purpose — the two have different
audiences (other nodes vs. external clients) and evolve independently.
`internal/raft/rpc` is the thin adapter layer: it converts between raft's
plain Go structs and the generated protobuf types, so `internal/raft` itself
never imports gRPC or protobuf.

## Algorithm notes / subtleties actually implemented

- **Election Safety**: RequestVote only grants a vote if the candidate's log
  is at least as up to date (by last-log-term, then last-log-index) as the
  voter's own. Verified directly by
  `TestElectionSafetyAcrossPartitionAndReconnect`, which asserts no two
  nodes ever claim leadership in the same term — including while a
  partitioned old leader is still running and legitimately believes itself
  to be leader (this is correct Raft behavior, not a bug: safety comes from
  the fact that it can never gather a majority to commit anything, not from
  it stepping down on its own).
- **Leader Completeness / commit rule (paper §5.4.2)**: a leader only
  advances its commit index by counting replicas for entries from its own
  current term. Older-term entries commit only indirectly, once a
  current-term entry that comes after them is itself committed. Getting
  this wrong is a classic Raft bug (see the paper's Figure 8) — implemented
  in `maybeAdvanceCommitIndexLocked` (replication.go).
- **Conflict resolution**: AppendEntries returns a `ConflictIndex` hint so a
  leader can back a follower's `nextIndex` off directly to the first index
  of the conflicting term, rather than one entry at a time.
  `TestLogConflictIsResolvedOnReconnect` exercises this by partitioning a
  minority node away, committing an entry on the majority side, then
  reconnecting and confirming the once-isolated node converges rather than
  diverging.
- **Overwritten-proposal detection**: `Propose` remembers the term it
  proposed an entry under; if that log index later holds a different term
  (a new leader overwrote it after a partition), `Propose` returns
  `ErrNotLeader` instead of falsely reporting success.
- **Crash-safe persistence**: every state change that affects an RPC's
  correctness (`currentTerm`, `votedFor`, log entries) is persisted before
  the RPC handler returns, per the paper's requirement. The log file uses
  the same framed-length + CRC32 + torn-write-truncation technique as
  `internal/storage/wal`; `currentTerm`/`votedFor` use the same
  temp-file/fsync/rename/fsync-dir atomic-replace protocol
  `internal/storage/lsm` uses for SSTable flushes.

## What's real vs. simplified

**Real**: leader election (single-round-trip RequestVote, randomized
timeouts, split-vote handling via retry), log replication with conflict
resolution, the current-term-only commit rule, crash-safe persistence of
term/vote/log, a real gRPC transport verified over real sockets, and a real
3-node cluster (`internal/raft`, `internal/raft/rpc`, `tests/integration`)
electing a leader, replicating a write, rejecting writes on followers with a
leader hint, and failing over — all demonstrated by automated tests **and**
manually verified against the compiled `cmd/node` binary (3 processes,
`kill -9` the leader, confirm re-election and continued writes).

**Simplified / explicitly deferred**:
- No snapshotting or log compaction — the log grows unboundedly. Fine at
  this project's scale; noted as future work.
- No dynamic cluster membership changes — `peers` is static, set at startup
  via config. Adding/removing nodes means restarting with a new config.
- No leader-forwarding at the gateway layer yet — a client must know which
  node is currently the leader (the `/v1/cluster` endpoint on any node
  reports it, and rejected writes name the current leader). Milestone 4
  (the API gateway) is where this becomes transparent to clients.
- Reads (`GET`) are served from each node's local applied state, not routed
  through Raft — this means a follower can serve a slightly stale read
  during replication lag. This matches the spec's own EVENTUAL/STRONG
  consistency model, which isn't enforced at the read path yet (workload-
  aware consistency is Milestone 6).

## Trying it yourself

```bash
make cluster   # starts node-1 (:8080), node-2 (:8081), node-3 (:8082)
curl localhost:8080/v1/cluster   # shows mode, term, this node's role, and the current leader

# write to whichever node the above said is LEADER, e.g.:
curl -X PUT localhost:8081/v1/kv/event:mackey \
  -d '{"value":{"title":"Purdue Basketball"},"consistency":"STRONG"}'

curl localhost:8080/v1/kv/event:mackey   # replicated to a different node

make stop
```

See `docs/architecture.md` for how this fits into the rest of the system.
