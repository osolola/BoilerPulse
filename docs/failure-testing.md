# Failure & Chaos Testing

Milestone 9 (spec §23/§34/§48): every node runs a small admin server for
injecting real failures — killing the process, adding RPC latency, dropping
a fraction of RPCs, or fully partitioning a node from its peers — plus
tooling on top of it (`scripts/chaos.sh`, the gateway's admin proxy, and the
`/cluster` page's chaos controls). This is deliberately at the *transport*
layer, not a mock: partitioning a node really does make its outgoing and
incoming Raft RPCs fail, the same way a real network partition would.

## The pieces

- **`internal/raft/rpc.Faults`** — per-node fault state (partitioned,
  latency, drop rate), shared between that node's outbound `Transport`
  (client side) and inbound `Server` (server side), so a partition is
  bidirectional: the node can neither send nor receive Raft RPCs, matching
  a real network partition rather than a one-way failure.
- **`internal/admin`** — an HTTP server, mounted by `cmd/node` on a
  separate port (`admin_addr` in config / `BOILERPULSE_ADMIN_ADDR`), gated
  by a shared-secret bearer token (`BOILERPULSE_ADMIN_TOKEN`). Routes:
  `POST /kill`, `POST /fault`, `POST /restore`, `GET /status`. An empty
  configured token disables every route (503) rather than falling back to
  open access — admin endpoints must be opted into deliberately.
- **`internal/gateway`'s admin proxy** (`admin_proxy.go`) — forwards
  `/v1/admin/{nodeID}/{kill,fault,restore,status}` and an aggregate
  `GET /v1/admin/status` to the right node's admin server, using the same
  shared token. This is what the frontend talks to, so the browser only
  ever needs to know the gateway's origin, not each node's separate admin
  port.
- **`scripts/chaos.sh`** — a CLI that talks to each node's admin port
  directly (not through the gateway), so it keeps working even when the
  gateway itself is what you're testing. `restart` relaunches `./bin/node`
  with the same config, the same way `make cluster` originally started it.
- **`/cluster` page chaos controls** — a token-gated panel per node (kill,
  partition/heal, latency, drop rate, restore), polling
  `GET /v1/admin/status` through the gateway every 2s.
- **`tests/failure`** — spec §48's four scenarios, against real components
  (real disk, real Raft, real gRPC, real HTTP), not mocks.

## What `kill` actually does

`POST /kill` responds `202 Accepted`, then — in a goroutine, after the
response has had time to flush — calls `os.Exit(1)`. No graceful Raft
step-down, no clean shutdown: the same "no chance to clean up" scenario
every crash-recovery test elsewhere in this project (WAL, Raft log, cluster
failover) already exercises. Data on disk is exactly as durable as it was
before the kill — the WAL and Raft log are both crash-safe by construction
(see `docs/storage-engine.md`, `docs/raft.md`), not because `kill` does
anything special.

## The four scenarios (`tests/failure`, spec §48)

All four run against a real 3-node cluster built directly from
`internal/raft`, `internal/storage/lsm`, and `internal/api` — the same
components `cmd/node` assembles — so a "restart" is a real process
restart with real recovered state, not a fresh node.

1. **Kill a follower** (`TestKillFollowerSystemRemainsAvailable`): the
   leader still has a majority (2 of 3) without it — writes and reads
   continue uninterrupted.
2. **Kill the leader** (`TestKillLeaderElectsNewLeader`): the remaining 2
   of 3 nodes still form a majority and elect a successor; writes resume
   against the new leader.
3. **Kill 2 of 3 nodes** (`TestKillTwoOfThreeLosesQuorumWritesRejected`):
   the lone survivor can never win a majority (needs 2 of 3 votes, has
   only its own) — the test asserts it never claims leadership (no split
   brain) and that writes are rejected with `503 LEADER_UNAVAILABLE`.
4. **Restart the old leader**
   (`TestRestartOldLeaderRejoinsWithoutOverwritingCommittedData`): it must
   rejoin as a follower, catch up on everything committed while it was
   down, and keep its own pre-crash committed write — without ending up as
   a second leader.

Run them with `go test ./tests/failure/...` (or `-race`, same as the rest
of the suite).

### A real bug these tests found

The first version of scenario 4's test compared the restarted node's `GET`
response against the wrong JSON shape and failed consistently. Chasing it
down (see the fix in this milestone's history) initially looked like it
might be a real replication bug — instrumented runs showed the *durable*
pre-crash key reads back correctly within milliseconds of restart, but the
post-crash key (requiring real replication from the new leader) took up to
a few hundred milliseconds longer, long enough that a naive read of the
diff made it look broken. It turned out to be a test-assertion bug (a
`PUT` body's `{"value": "x"}` envelope isn't the same string as the stored
value `"x"`), not a Raft bug — but finding that out required actually
instrumenting a real run rather than trusting the first failure's
symptom.

A second real bug *did* turn up during this milestone, in the frontend
integration rather than Raft itself: see below.

## Manually verified (real cluster, real browser)

Everything below was run against a real `make cluster` (3 Raft nodes +
gateway + ingest, real OS processes) and a real headless-browser session
against the actual Next.js dev server — not curl-only, not mocked.

```
$ make cluster
cluster started: node-1 :8080, node-2 :8081, node-3 :8082, gateway :8090, ingest
admin/chaos: node-1 :7080, node-2 :7081, node-3 :7082, token in .cluster/admin.token

$ scripts/chaos.sh status
== node-1 == {"faults":{"partitioned":false,...},"raft":{"state":"FOLLOWER",...}}
== node-2 == {"faults":{"partitioned":false,...},"raft":{"state":"LEADER",...}}
== node-3 == {"faults":{"partitioned":false,...},"raft":{"state":"FOLLOWER",...}}

$ curl -X PUT localhost:8090/v1/kv/chaos-test -d '{"value":"before-kill"}'   # 204
$ scripts/chaos.sh kill node-2                                              # kills the leader
$ curl localhost:8090/v1/cluster
# node-1 now LEADER at term 3, node-2 "reachable":false, node-3 FOLLOWER

$ curl -X PUT localhost:8090/v1/kv/chaos-test2 -d '{"value":"during-downtime"}'  # 204 -- still available
$ scripts/chaos.sh restart node-2
$ curl localhost:8090/v1/cluster
# node-2 back as FOLLOWER at term 3

$ curl localhost:8081/v1/kv/chaos-test    # {"value":"before-kill",...}   -- own durable data survived
$ curl localhost:8081/v1/kv/chaos-test2   # {"value":"during-downtime",...} -- caught up via replication
```

Also verified `latency`, `drop`, `partition`/`heal`, and `restore-all`
against the live cluster via `scripts/chaos.sh` and via the gateway's
`/v1/admin/*` proxy directly with `curl`.

**The `/cluster` page's chaos controls were driven in a real headless
browser** (Playwright), not just typechecked: unlocked with the admin
token, applied 300ms of latency to a node, watched the status pill update
to `+300ms` on the next poll, then restored it and watched it go back to
`no faults`. Interestingly, injecting that latency was enough to visibly
destabilize the live cluster — the topology view above the chaos panel
showed a real election in progress (`node-2` briefly `CANDIDATE`) as a
direct, organic side effect of the injected fault, not something staged.

### A real bug this caught: CORS blocked the Authorization header

The first browser run failed every admin-proxy request with:

```
Access to fetch at 'http://localhost:8090/v1/admin/status' from origin
'http://localhost:3000' has been blocked by CORS policy: Request header
field authorization is not allowed by Access-Control-Allow-Headers in
preflight response.
```

`internal/gateway.Gateway.ServeHTTP` set `Access-Control-Allow-Headers:
Content-Type` — correct for the rest of the API, which never receives a
custom header from the browser, but the chaos controls are the first
thing in this project to send `Authorization` from client-side JS. The
browser's CORS preflight rejected it before the actual request ever went
out. Fixed by adding `Authorization` to the allow-list
(`internal/gateway/gateway.go`), with a regression test
(`TestCORSAllowsAuthorizationHeader`) asserting on the header value, not
just its presence. This is the second CORS bug this project has hit that
curl-based testing structurally cannot catch (the first was Milestone 8's
missing `Access-Control-Allow-Origin`) — curl doesn't implement CORS at
all, so anything gated by a preflight only shows up when something that
actually enforces it (a real browser) drives the request.

## What's simplified

- **Admin auth is a single shared secret, not per-operator credentials.**
  Fine for local dev and a personal demo; before this goes anywhere public,
  Milestone 11 needs to replace it with something that can be scoped,
  rotated, and audited per caller.
- **No rate limiting on admin routes.** The public KV API and gateway have
  token-bucket limiting (`docs/gateway.md`); `/v1/admin/*` and each node's
  admin server do not, since only a token holder can reach them at all
  today. Reconsider before Milestone 11.
- **`kill` is the only failure mode that's a real process death.**
  Latency/drop/partition are real at the RPC layer, but they're applied
  and cleared by talking to a *live* process — there's no fault that, say,
  corrupts a node's on-disk state or truncates a file mid-write. Milestone
  2/3's crash-safety tests cover that ground separately (torn WAL writes,
  torn Raft log writes), just not through this same admin-server path.
- **The frontend's admin token lives only in React state**, never
  persisted (not even `sessionStorage`) — deliberate, since this is
  opt-in destructive tooling, but it also means refreshing the page always
  re-locks the panel.

## Trying it yourself

```bash
make cluster                      # writes .cluster/admin.token
scripts/chaos.sh status
scripts/chaos.sh kill node-2
scripts/chaos.sh restart node-2
scripts/chaos.sh partition node-1
scripts/chaos.sh latency node-3 500
scripts/chaos.sh drop node-3 0.3
scripts/chaos.sh restore-all
make stop
```

Or open the frontend's `/cluster` page, unlock the chaos panel with the
token from `.cluster/admin.token`, and drive it from there.

See `docs/raft.md` for what's actually being disrupted, and
`docs/gateway.md` for the admin-proxy routes' place in the gateway's
overall request handling.
