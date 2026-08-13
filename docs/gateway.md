# API Gateway

`internal/gateway` (run via `cmd/gateway`) fronts a configured set of KV
nodes. It's the only piece of BoilerPulse that has a live, independently-
verified view of the *whole* cluster at once — every node only knows its own
Raft status plus a best-known leader hint.

## Responsibilities (spec §24)

- **Leader discovery**: polls every configured node's `GET /v1/cluster`
  every `LeaderRefreshInterval` (default 500ms), and once synchronously on
  `Start`. Because a partitioned node can legitimately still believe an old
  term's leader is current (see `docs/raft.md`), the gateway picks the
  leader reported by whichever node has the *highest* term.
- **Write routing**: `PUT`/`DELETE /v1/kv/{key}` are proxied to the cached
  leader. If none is known yet, or the cached leader turns out to be stale
  (it answers with its own honest `503 LEADER_UNAVAILABLE`, e.g. right after
  losing an election), the gateway does one synchronous refresh and retries
  once against whatever it now believes is the leader — this is what makes
  failover transparent to the client instead of requiring the client to
  retry itself.
- **Read distribution**: `GET /v1/kv/{key}` round-robins across every
  configured node, skipping ahead to the next node (up to one full pass) on
  a connection failure.
- **Cluster status**: `GET /v1/cluster` reports real per-node reachability
  (from the same poll used for leader discovery) plus each node's last-known
  role and term — genuinely richer than any single node can report on its
  own, since a node has no way to independently verify a peer's liveness
  beyond Raft's own heartbeats.
- **Rate limiting**: a token-bucket limiter per client IP (spec §52/§67-A),
  applied to `/v1/kv/*` before any request reaches a node.

## What's simplified

- Read distribution is blind round-robin, not consistency-aware — the spec's
  STRONG/CRITICAL consistency model (§13) isn't enforced at the read path
  yet anywhere in the system (noted in `docs/raft.md` too); that's
  Milestone 6.
- No caching at the gateway (Milestone 6 — adaptive caching is explicitly a
  workload-aware feature, not a generic proxy cache).
- Node membership is static config, matching Raft's own static `peers` — no
  dynamic node discovery.

## Trying it yourself

```bash
make cluster   # 3 Raft nodes + the gateway, all in one command now
curl localhost:8090/v1/cluster
# {"mode":"RAFT_GATEWAY","leader_id":"node-2","nodes":[...,"reachable":true,"role":"LEADER","term":1,...]}

curl -X PUT localhost:8090/v1/kv/event:mackey -d '{"value":{"title":"Purdue Basketball"}}'
curl localhost:8090/v1/kv/event:mackey   # works regardless of which node is leader

make stop
```

Manually verified: writes routed correctly to the leader, reads distributed
across all three nodes with consistent values, and — after `kill -9` on the
leader node directly — the gateway's next poll detected the dead node
(`"reachable":false`), picked up the newly-elected leader, and continued
accepting writes without any client-visible interruption beyond the single
failover window.

See `docs/raft.md` for the consensus layer underneath this, and
`docs/architecture.md` for how it all fits together.
