# Architecture

## Today (Milestone 9)

A 3-node Raft cluster, each `cmd/node` running the KV HTTP API on a durable
WAL+SSTable storage engine plus a Raft node replicating writes over gRPC —
fronted by `cmd/gateway`, which finds the leader, distributes reads, fails
over transparently, rate-limits clients, caches, tracks workload state, and
predicts traffic. A client can still talk directly to a node if it wants to
(and must, to observe an individual node's own view of its Raft state), but
doesn't have to know who the leader is.

`cmd/ingest` generates the actual workload: it runs `internal/events`'s
`SimulatorSource` on an interval, normalizes what comes out (urgency
classification, traffic-multiplier estimation), and `POST`s each event
through the gateway. Events are stored as ordinary KV entries under
`event:<id>` — `GET /v1/events` is a prefix `Scan` over the same engine, not
a second storage system — and an event's urgency directly picks its
consistency level (CRITICAL events get CRITICAL consistency), the first
concrete tie-in to the spec's workload-aware consistency model (§13).

The gateway closes the loop: `internal/workload` watches every request it
proxies (rate + per-key activity) to drive an automatic
NORMAL→ELEVATED→HIGH_TRAFFIC→CRITICAL mode (`GET /v1/workload`), and a
CRITICAL-urgency event forces CRITICAL mode directly, matching §26.
`internal/cache` caches `GET /v1/kv/{key}` reads — except CRITICAL-
consistency data, which is never cached, per §25 — and gets invalidated on
every write to that key. `internal/prediction` — a linear regression
trained from scratch on documented synthetic data — predicts RPS/
confidence/peak-time/recommended-nodes for any event via
`POST /v1/predict`, automatically logging a prediction for every event
`cmd/ingest` posts. And `frontend/` is a real dashboard on top of all of
this: every page (cluster topology, event feed, live metrics, a
prediction form) reads real data through the gateway — verified in an
actual browser, not just built (see `docs/frontend.md`).

Every `cmd/node` also optionally runs a small `internal/admin` HTTP server
on a separate port — token-gated kill/latency/packet-drop/partition
controls (spec §23/§34) that act on the real gRPC transport, not a mock of
it (`internal/raft/rpc.Faults` is shared between a node's outbound
`Transport` and inbound `Server`, so a "partition" really is
bidirectional). The gateway proxies these per-node (`/v1/admin/*`) so
neither `scripts/chaos.sh` nor the `/cluster` page's chaos controls need
to know each node's separate admin port. See `docs/failure-testing.md`.

```
   frontend / curl
         │
         ▼
  ┌──────────────────┐  routes writes to the leader, distributes reads,
  │  cmd/gateway     │  rate-limits, caches, tracks workload, predicts
  │  internal/gateway│
  └────────┬─────────┘
           │ HTTP
           ▼
  ┌───────────────────────┐        gRPC (RequestVote/AppendEntries)
  │  cmd/node              │◄───────────────────────────────┐
  │  ┌──────────────────┐  │                                 │
  │  │ internal/api     │  │   PUT/GET/DELETE /v1/kv/{key}   │
  │  │  Proposer? ───┐  │  │   GET /v1/cluster, GET /healthz │
  │  └───────────────┼──┘  │                                 ▼
  │                  ▼     │                        ┌───────────────────┐
  │  ┌──────────────────┐  │                        │  cmd/node (peer)  │
  │  │ internal/raft    │  │◄──────────────────────► │  ...same shape    │
  │  │  Node: election, │  │      gRPC               └───────────────────┘
  │  │  replication     │  │
  │  └────────┬─────────┘  │
  │           │ commit+apply
  │           ▼             │
  │  ┌──────────────────┐  │
  │  │ internal/storage │  │
  │  │  /lsm.Engine     │  │   memtable + WAL + SSTables (Milestone 2)
  │  └──────────────────┘  │
  └───────────────────────┘
  <data_dir>/wal.log, <data_dir>/sstables/*.sst, <data_dir>/raft/raft-log.bin
```

`internal/storage.Engine` is the seam that let Milestones 1-3 layer cleanly:
`MemStore` (Milestone 1) and `lsm.Engine` (Milestone 2) both implement it,
and Milestone 3's Raft doesn't change that interface at all — it sits in
`internal/api` as an optional `Proposer` that intercepts writes *before* they
reach the engine, replicates them via consensus, and only then applies them
to the same `Engine` interface. See `docs/storage-engine.md` for the KV
on-disk format and `docs/raft.md` for the consensus design.

## Target

```
                    ┌─────────────────────┐
                    │   Next.js Frontend  │  (done — frontend/, docs/frontend.md)
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    API Gateway      │  (done — cmd/gateway, internal/gateway)
                    └──────────┬──────────┘
                               │
              ┌────────────────┼─────────────────┐
              ▼                ▼                 ▼
       ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
       │ KV Node 1   │  │ KV Node 2   │  │ KV Node 3   │
       │   Leader    │  │  Follower   │  │  Follower   │
       └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
              │                │                 │
              └────────────────┼─────────────────┘
                          Raft Replication (done — internal/raft, pkg/protocol gRPC)
                               │
                               ▼
                    WAL + Memtable + SSTables (done — internal/storage/lsm)
```

## Why this layering

- `internal/storage.Engine` is defined independently of Raft so the API
  layer's storage access never had to change when consensus was added —
  `MemStore`, then `lsm.Engine`, and now a Raft-replicated write path all
  satisfy (or write through to) the same three-method interface.
- Node-to-node RPC is gRPC/protobuf in `pkg/protocol` (`internal/raft/rpc`
  adapts it to `internal/raft`'s transport-agnostic core), kept separate
  from the plain HTTP/JSON client-facing API in `internal/api` — the two
  have different audiences and don't share a wire format.
- `internal/raft.Node` depends only on `Transport`/`Storage`/`StateMachine`
  interfaces, not on gRPC or the KV engine directly — this is what let the
  algorithm itself (election, replication, conflict resolution) be tested
  with a fast in-memory fake network, independently of whether the real gRPC
  transport or file-based persistence work. See `docs/raft.md`.
- `docker-compose.yml` runs the real topology: `kv-node-1/2/3`, `gateway`,
  `ingest`, and `frontend` — the same six-process shape `make cluster` runs
  as local OS processes, just containerized.

See `docs/roadmap.md` for what's implemented vs. planned, milestone by milestone.
