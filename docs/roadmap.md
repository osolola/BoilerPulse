# Roadmap

Milestones as defined in the project spec (§57). Checked items are actually
implemented and tested — nothing here is marked done on the basis of intent.

- [x] **Milestone 1 — Basic KV Store**: GET/PUT/DELETE, in-memory storage, HTTP API.
      `curl → KV store → response` works today.
- [x] **Milestone 2 — Persistence**: WAL, recovery, memtable, SSTables.
      `kill -9 → restart → data survives` works today, verified by tests and a manual smoke test.
- [x] **Milestone 3 — Raft**: node states, leader election, heartbeats, replicated logs, commit index.
      `3 nodes → leader elected → write → replicated` works today, verified by tests and manually (kill -9 the leader, watch re-election).
- [x] **Milestone 4 — Distributed Reads/Writes**: gateway, leader routing, follower reads, cluster metadata.
      Write/read through the gateway on any node's behalf, transparent failover, works today.
- [x] **Milestone 5 — Campus Event Model**: Event struct, ingestion, simulator, normalization, urgency classification.
      `cmd/ingest` generates synthetic events → normalizes → POSTs through the gateway → replicated across the cluster, works today.
- [x] **Milestone 6 — Workload Engine**: workload modes, hot-key detection, traffic monitoring, adaptive caching.
      Gateway-level LRU cache (never CRITICAL data), live hot-key/RPS tracking, NORMAL→CRITICAL mode transitions — works today.
- [x] **Milestone 7 — Traffic Prediction**: synthetic historical dataset, prediction model, recommended capacity.
      `POST /v1/predict` returns real predicted_rps/confidence/peak_time/recommended_nodes from a model trained (and validated to actually beat a baseline) on documented synthetic data, works today.
- [x] **Milestone 8 — Frontend**: dashboard, events, cluster topology, metrics, simulation controls.
      Every page wired to real backend data (no simulated data anywhere on the site), verified in an actual browser, works today. `/simulation` is still an honest placeholder — that's Milestone 10's job.
- [x] **Milestone 9 — Failure Simulation**: kill node, restart node, latency, packet loss, network partitions.
      A token-gated admin server per node, a gateway proxy, a CLI (`scripts/chaos.sh`), and `/cluster` page chaos controls, all driving real gRPC-layer faults — verified against a real cluster and a real browser, works today.
- [x] **Milestone 10 — Benchmarking**: normal/finals/athletics/emergency/hot-key/failure scenarios with real measurements.
      `simulator/` + `cmd/simulator` generate real HTTP load and measure what actually happened; found and fixed a real Raft replication concurrency bug, and found (documented, not fixed) a real WAL-fsync write-throughput ceiling — works today.
- [~] **Milestone 11 — Deployment**: public demo with failure-injection endpoints gated behind admin auth.
      Everything short of an actual live URL is done: real Dockerfiles + Compose topology, configurable CORS (no more hardcoded `*`), rate-limited admin routes, and a real deployment/hardening checklist (`docs/deployment.md`). No public instance exists — provisioning one means picking and paying for a host, a decision for this repo's owner, not something to do unilaterally. Not marked `[x]`; this README/roadmap does not claim a live demo exists.

## What "Milestone 1 complete" means concretely

- `internal/storage`: `Engine` interface + `MemStore` (map + mutex, tombstones, TTL expiry) — kept as a fast, ephemeral engine for tests.
- `internal/api`: KV HTTP API (`PUT`/`GET`/`DELETE /v1/kv/{key}`, `GET /v1/cluster`, `GET /healthz`) with the spec's error envelope shape.
- `internal/config`, `internal/logging`: YAML+env config, structured JSON logs.
- `cmd/node`: wires the above into a runnable single-node binary.
- Frontend: Next.js/TypeScript/Tailwind app with all six route shells (`/`, `/events`, `/cluster`, `/simulation`, `/metrics`, `/about`); `/` and `/cluster` poll the real node over HTTP, the rest are explicit "not yet implemented" placeholders.
- Tests: unit tests for storage and config, handler tests for the API, one HTTP-level integration test. `go vet`, `gofmt`, `go test ./...`, and `go test -race ./...` all pass.
- CI, Dockerfiles, and a pass-1 `docker-compose.yml` (single node + frontend, not yet the target multi-node topology).

## What "Milestone 2 complete" means concretely

- `internal/storage/wal`: durable, checksummed, append-only write-ahead log with torn-write-safe replay.
- `internal/storage/sstable`: on-disk sorted-table format (data block + index + footer), writer + reader.
- `internal/storage/lsm`: `Engine` (a `storage.Engine`) wiring WAL + memtable + SSTables, with a crash-safe atomic flush protocol and synchronous full-table compaction that drops tombstones and expired entries.
- `cmd/node` now runs on `lsm.Engine` instead of the ephemeral `MemStore`; `internal/config` gained a `data_dir` setting.
- Tests: unit tests per package (wal, sstable, lsm) including two explicit crash-recovery tests and two compaction tests, an HTTP-level "kill the process, restart, data survives" integration test, and a manual `kill -9` smoke test against the real compiled binary. All clean under `-race`.
- `docs/storage-engine.md` documents the on-disk formats, the crash-safety protocol, and the deliberate simplifications (synchronous flush/compaction, full-table compaction, no manifest file).

## What "Milestone 3 complete" means concretely

- `internal/raft`: hand-rolled Raft — node states, randomized election timeouts, RequestVote/AppendEntries (leader and follower sides), the current-term-only commit rule, log-conflict resolution with a backoff hint, crash-safe persistence of term/vote/log, and overwritten-proposal detection. Transport- and storage-agnostic (`Transport`/`Storage`/`StateMachine` interfaces), so the algorithm is tested with a fast in-memory fake network independently of the real gRPC transport.
- `pkg/protocol/raft.proto` + generated code, `internal/raft/rpc`: real gRPC transport (client + server) for RequestVote/AppendEntries, separate from the client-facing HTTP/JSON KV API.
- `internal/api`: an optional `Proposer` interface — when set, PUT/DELETE route through Raft consensus instead of writing directly to the local engine; a non-leader node rejects writes with `LEADER_UNAVAILABLE` and names the current leader; `/v1/cluster` reports real role/term/leader when Raft is enabled.
- `internal/config`: `raft_addr` and `peers` (static, config-driven cluster membership).
- `cmd/node`: runs a Raft node alongside the KV API when `peers` is configured; behaves exactly like the Milestone 2 single-node binary when it isn't.
- `make cluster` / `make stop`: run/stop a real local 3-node cluster (`configs/cluster/*.yaml`).
- Tests: 9 algorithm tests (election, failover, replication, partition recovery, log-conflict resolution, an explicit election-safety check, persistence-across-restart) against a fake network; file-storage persistence + crash-recovery tests; a real-gRPC-over-localhost test; two full 3-node cluster integration tests (election + replicated write, and failover-then-continues-accepting-writes). All clean under `-race`. The same failover scenario was also verified manually against the compiled binary.
- `docs/raft.md` documents the algorithm decisions actually implemented (election safety, the §5.4.2 commit rule, conflict resolution, overwritten-proposal detection) and what's still simplified (no snapshotting, static membership, no gateway-level write forwarding yet).

## What "Milestone 4 complete" means concretely

- `internal/gateway`: `Gateway` (an `http.Handler`) that polls every configured node's `/v1/cluster` (picking the highest-term leader, since a partitioned node can legitimately still claim an old term's leadership), proxies `PUT`/`DELETE` to the current leader with one refresh-and-retry on a stale cache, round-robins `GET`s across all nodes with failover to the next node on a connection error, reports a real gateway's-eye cluster view (per-node reachability + role + term, not just one node's self-report), and rate-limits per client IP with a token bucket.
- `cmd/gateway`: thin binary wiring `internal/gateway` to config (`configs/gateway.yaml`, `configs/docker/gateway.yaml`).
- `internal/api`: three more error codes (`QUORUM_UNAVAILABLE`, `NODE_UNAVAILABLE`, `RATE_LIMITED`) completing the spec's §54 taxonomy, exported for `internal/gateway` to reuse the same error envelope.
- `make cluster` now starts the gateway alongside the 3 nodes; `docker-compose.yml` gained a real `gateway` service, and the frontend now points at it instead of a single node.
- Tests: 9 gateway tests (leader routing, stale-leader retry, read distribution, read failover, cluster-status reachability, rate-limit burst/refill, 429 over HTTP) using fake node HTTP servers — no mocking framework, just `httptest`. All clean under `-race`.
- Manually verified against a real 3-node cluster + gateway: write routed to the actual leader, reads returned consistent values from all three nodes, and killing the leader process outright was absorbed transparently — the gateway's next poll saw the dead node, picked up the new leader, and continued accepting writes with no client-visible error.
- `docs/gateway.md` documents the design and what's still simplified (round-robin reads aren't consistency-aware yet, no gateway-side caching — that's Milestone 6).

## What "Milestone 5 complete" means concretely

- `internal/events`: normalized `Event` model (spec §6), the `EventSource` interface, `SimulatorSource` (a small template catalog, 1-3 random events per fetch), and the validate → normalize → classify → estimate-traffic pipeline — classification/estimation only fill in zero values, never override a source that already knows better.
- `storage.Engine` gained `Scan(prefix)`, implemented in both `MemStore` and `lsm.Engine` (oldest-sstable-to-newest-to-memtable merge, same precedence as `Get`) — added specifically so events (and anything else prefix-addressable later) can be listed without a second storage system.
- `internal/api`: `POST /v1/events` (normalize + store under `event:<id>`, with consistency chosen by `consistencyForUrgency` — CRITICAL urgency gets CRITICAL consistency, tying directly into the spec's workload-aware consistency model) and `GET /v1/events` (sorted list via `Scan`); `internal/gateway` routes both the same way it already routes `/v1/kv/*`.
- `cmd/ingest`: runs `SimulatorSource` on an interval, normalizes, POSTs to the gateway. A source failing (or the target being briefly unreachable) only skips that round — it never takes ingestion or the KV cluster down for the other.
- `make cluster` now also starts `ingest`; `docker-compose.yml` gained an `ingest` service.
- Tests: 21 tests in `internal/events` (validation, classification, traffic estimation, confidence, simulator output validity, context cancellation, batch-skip-on-one-bad-event), Scan tests in both storage engines, 6 new `internal/api` handler tests, a gateway routing test for `/v1/events`. All clean under `-race`.
- Manually verified end to end: `make cluster` running ingest generating synthetic events every few seconds, normalized (urgency classified, traffic multiplier estimated), POSTed through the gateway to the actual Raft leader, and visible via `GET /v1/events` on every node — including nodes queried directly, confirming Raft replication of event data exactly like any other write.
- `docs/event-ingestion.md` documents the pipeline and the urgency→consistency mapping.

## What "Milestone 6 complete" means concretely

- `internal/workload`: the `Mode` state machine (NORMAL/ELEVATED/HIGH_TRAFFIC/CRITICAL), a millisecond-bucketed `RequestMonitor` (sliding-window RPS) and `HotKeyTracker` (per-key sliding-window counts, threshold-based), and an `Engine` combining them — mode is purely reactive to RPS except an explicit `SignalCritical` (fed by CRITICAL-urgency events) that holds CRITICAL for a minimum duration.
- `internal/cache`: a standard `container/list`-backed LRU with hit/miss/eviction stats, capacity-bounded, safe for concurrent use.
- `internal/gateway`: wired both in — caches `GET /v1/kv/{key}` responses (never CRITICAL consistency), invalidates on write, records every proxied request into the workload engine, and inspects `POST /v1/events` *responses* (the normalized event, not the client's request) to detect CRITICAL urgency and force workload mode. New `GET /v1/workload` endpoint reports mode/RPS/hot-keys/cache-stats.
- A real bug found and fixed along the way: the gateway's stale-leader retry re-read `r.Body`, which is a stream — already drained by the first attempt — so a retried write silently sent an **empty body** to the new leader. Fixed by reading the body once and threading the bytes through every attempt; a dedicated regression test (`TestHandleWriteRetryCarriesOriginalBody`) checks the actual bytes the new leader receives, not just the status code (the existing fake-node tests didn't validate body content, which is exactly how this slipped past them originally).
- Tests: 15 tests in `internal/workload`, 9 in `internal/cache`, 8 new gateway tests (cache hit/miss/invalidation, CRITICAL bypass, hot-key/RPS reporting, CRITICAL-event-triggers-CRITICAL-mode, the retry-body regression test). All clean under `-race`.
- Manually verified against a real 3-node cluster + gateway: cache hit/miss (`X-Cache` header), cache invalidation on write, hot-key detection after a request burst, and posting an EMERGENCY event flipping `/v1/workload`'s mode to CRITICAL — plus confirming a CRITICAL-consistency key is never cached across repeated GETs.
- `docs/workload-model.md` documents the design and the simplifications (no hysteresis on mode transitions, no dynamic replica scaling, gateway-only caching).

## What "Milestone 7 complete" means concretely

- `internal/prediction`: `Features` (one-hot event type, attendance, ordinal urgency, hour-of-day, day-of-week, duration) extracted from a normalized `Event`; `GenerateSyntheticDataset` producing labeled samples from a documented ground-truth formula plus noise — explicitly not real data (§67-C); `Model`, a multiple linear regression trained from scratch via batch gradient descent on standardized features (no ML library).
- `internal/gateway`: trains a `prediction.Model` on 2000 synthetic samples at startup; `POST /v1/predict` returns a prediction for a posted event without storing it; every successful `POST /v1/events` also triggers a logged prediction for the normalized event (observable, not yet acted on — proactive capacity changes are still out of scope, matching Raft's still-static membership).
- A real bug found and fixed along the way: `numFeatures()` said "4 trailing scalar features" but `vector()` actually appends 5 (attendance, urgency, hour, weekday, duration) — an off-by-one that panicked with an index-out-of-range the moment a test actually trained the model. A dedicated `TestVectorLengthMatchesNumFeatures` regression test locks the invariant in.
- Tests: 12 tests in `internal/prediction`, including the model beating a mean-only baseline on held-out synthetic data (i.e. verifying it's actually learning something) and correctly ordering large-vs-small and CRITICAL-vs-NORMAL events; 2 new gateway tests for `/v1/predict`. All clean under `-race`.
- Manually verified against a real 3-node cluster + gateway: a large athletics event predicted ~54k RPS / 27 recommended nodes vs. a small dining event's ~79 RPS / 1 node, and real `cmd/ingest`-generated events (WEATHER/EMERGENCY → CRITICAL, ~16-17k RPS predicted) showing up as logged predictions on the gateway.
- `docs/prediction.md` documents the model, the synthetic-data honesty requirement, and what's still simplified (no acting on predictions, no geographic features, no real historical data).

## What "Milestone 8 complete" means concretely

- `frontend/src/lib/api.ts` now speaks the gateway's full API (cluster, workload, events, predict), defaulting to the gateway (`:8090`) instead of a single node; `usePoll.ts` is a generic polling hook replacing the earlier node-specific `useClusterStatus`.
- All six pages: `/` (system status + workload mode + upcoming events + a live `POST /v1/predict` form), `/cluster` (real topology: leader highlighted, per-node reachability/term, no graph-viz library needed for three nodes), `/events` (real event feed, capped and sorted), `/metrics` (Recharts: RPS history, hot keys, cache stats), `/about` (implemented/planned lists kept honest). `/simulation` stays an explicit "not yet implemented" placeholder — `cmd/simulator` is Milestone 10.
- **Actually browser-tested**, not just built: every page driven with headless Chromium (Playwright, installed temporarily for this pass and removed afterward) against a real running cluster, checking for console errors and screenshotting each page.
- Two real bugs found this way, that `curl` and code review both missed: (1) **CORS** — neither the gateway nor a node sent `Access-Control-Allow-Origin`, silently blocking every browser fetch (curl never enforces CORS); fixed in both `internal/gateway` and `internal/api`, with tests. (2) **Unbounded event list** — after enough ingest, `/events` rendered 300+ rows as one ~22,000px page; fixed with a 50-item cap and a "showing X of Y" note.
- Tests: 4 new backend tests (CORS headers present, OPTIONS preflight) across `internal/api` and `internal/gateway`. All clean under `-race`. Frontend: `npm run lint` and `npm run build` (TypeScript strict) both clean.
- `docs/frontend.md` documents the pages, the design choices (why no graph-viz library, how the RPS chart's "history" works given the backend only reports current RPS), and the two bugs found during real browser verification.

## What "Milestone 9 complete" means concretely

- `internal/raft/rpc.Faults`: per-node fault state (partitioned, latency,
  drop rate), shared between that node's outbound `Transport` and inbound
  `Server` so a partition is genuinely bidirectional — the same object
  gates both `SendRequestVote`/`SendAppendEntries` (client side) and
  `RequestVote`/`AppendEntries` (server side).
- `internal/admin`: an HTTP server mounted by `cmd/node` on a separate,
  optional port (`admin_addr`), gated by a shared-secret bearer token
  (`BOILERPULSE_ADMIN_TOKEN`) — `POST /kill` (responds, then exits
  ungracefully), `POST /fault`, `POST /restore`, `GET /status`. An empty
  configured token disables every route (503), never falls back to open
  access.
- `internal/gateway/admin_proxy.go`: `GET /v1/admin/status` (aggregated
  across every node, tolerating individual unreachability) and
  `/v1/admin/{nodeID}/{kill,fault,restore,status}`, forwarding to the
  right node's admin server with the gateway's own configured token — the
  frontend only ever needs to know the gateway's origin.
- `scripts/chaos.sh`: a portable (bash 3.2-compatible — no associative
  arrays, since macOS ships 3.2 by default) CLI wrapper: `status`, `kill`,
  `restart` (relaunches `./bin/node` the same way `make cluster` does),
  `partition`/`heal`, `latency`, `drop`, `restore`/`restore-all`. Talks to
  each node's admin port directly, not through the gateway, so it works
  even when the gateway is the thing under test.
- `frontend/src/app/cluster/chaos-controls.tsx`: a token-gated panel per
  node on the `/cluster` page (kill, partition/heal, latency, drop-rate,
  restore), polling the gateway's aggregated admin status every 2s. The
  token lives only in React state, never persisted.
- `tests/failure`: spec §48's four scenarios (kill a follower, kill the
  leader, kill 2 of 3 nodes, restart-and-rejoin) against a real 3-node
  cluster built directly from `internal/raft`, `internal/storage/lsm`, and
  `internal/api` — the same components `cmd/node` assembles, so a
  "restart" is a real process restart with real recovered state.
- Config: `admin_addr` per node (`configs/cluster/*.yaml`,
  `configs/docker/*.yaml`) and `admin_addr` per gateway-known node
  (`configs/gateway.yaml`, `configs/docker/gateway.yaml`);
  `BOILERPULSE_ADMIN_TOKEN` wired through `.env.example`,
  `docker-compose.yml`, and `make cluster` (which defaults to
  `dev-chaos-token` for local convenience and writes it to
  `.cluster/admin.token`).
- Two real bugs found and fixed along the way: (1) a **test-assertion
  bug**, not a Raft bug — the restart-scenario test compared a decoded
  `value` field against the original PUT envelope string instead of the
  actual stored value, making it look like replication was broken when it
  wasn't; caught by instrumenting a real run rather than trusting the
  first failure's symptom. (2) A **real CORS bug** — the gateway's
  `Access-Control-Allow-Headers` only listed `Content-Type`, so every
  browser-side admin-proxy fetch (the first thing in this project to send
  a custom `Authorization` header from client-side JS) failed CORS
  preflight; found by actually driving the `/cluster` page in a real
  headless browser, not curl, which structurally cannot enforce CORS.
- Tests: 9 `internal/raft/rpc` fault tests, 10 `internal/admin` tests
  (including an injectable `exit` func so the kill test doesn't take down
  the test binary, and a nil-`*raft.Node` guard for single-node configs),
  10 `internal/gateway` admin-proxy tests, 4 `tests/failure` scenario
  tests, plus a CORS regression test. All clean under `-race`.
- Manually verified against a real `make cluster` (write → kill leader →
  confirm failover → write during downtime → restart → confirm rejoin and
  catch-up) via `scripts/chaos.sh` and the gateway's `/v1/admin/*` proxy
  directly with `curl`, and the frontend chaos controls driven in a real
  headless browser (unlock → apply latency → watch the status pill update
  → restore), which also showed the injected latency organically
  triggering a real election in the live cluster.
- `docs/failure-testing.md` documents the design, the four scenarios, both
  bugs found, and what's still simplified (single shared admin secret, no
  rate limiting on admin routes, `kill` is the only fault that's a real
  process death).

## What "Milestone 10 complete" means concretely

- `simulator/`: `Scenario` (baseline→peak→baseline RPS curve via linear
  ramps, read/write mix, hot-key concentration) with five named scenarios
  (normal, finals, athletics, emergency, hotkey); `Generator` — a real HTTP
  load generator that primes a warmup key set, dispatches requests
  following the scenario's target-RPS curve (a ticker-based scheduler
  bounded by a concurrency cap), and produces a `Report` (achieved RPS,
  latency p50/p95/p99/max, error rate, status-code breakdown) from what
  actually happened, never an estimate.
- `cmd/simulator` (`boilerpulse-sim`): the CLI — `-scenario`, `-target`,
  `-topology`, `-out`, `-inject-failure` (kills the current leader
  partway through, via the gateway's admin proxy, to measure a real
  failover's cost alongside steady-state throughput).
- `configs/cluster/gateway-benchmark.yaml`: a dedicated higher-rate-limit
  gateway config for benchmark runs — the default gateway's per-client-IP
  rate limit is correct for production but measures the wrong thing when
  all load comes from one load-generator IP; documented, not hidden.
- Two real findings, not invented numbers: (1) a genuine Raft replication
  concurrency bug — concurrent proposals used to spawn one goroutine (and
  one `AppendEntries` RPC) per peer per proposal with no serialization,
  flooding peers and starving heartbeats badly enough to destabilize a
  healthy leader under a write burst; **fixed** by serializing replication
  per peer through a coalescing trigger channel
  (`internal/raft/replication.go`), with a deterministic regression test
  (`TestConcurrentProposalsCoalesceReplicationPerPeer`). (2) A real
  write-throughput ceiling from the WAL's per-write, lock-serialized
  `fsync` — measured at roughly 50-80 sustained writes/sec before error
  rate climbs sharply; **documented**, not fixed (the real fix is
  group-commit batching, a bigger structural change deserving its own
  focused pass, not a same-day change bundled into a benchmarking
  milestone).
- Tests: 9 `simulator` unit tests (scenario curve math, load generator
  against a fake server, warmup priming, failure-injector scheduling and
  error handling) and 1 new `internal/raft` regression test. All clean
  under `-race`.
- Real measured results: single-node and 3-node runs across all five
  scenarios, plus one failure-injection run, in `benchmarks/results/`.
  `docs/benchmarking.md` documents the methodology, the full results
  table, and both findings above in detail.

## What Milestone 11 covers (and doesn't)

- Two real hardening fixes, both with tests: **configurable CORS**
  (`internal/api.Server.SetAllowedOrigin`,
  `internal/gateway.Options.AllowedOrigin`, wired through
  `BOILERPULSE_CORS_ORIGIN`/`cors_origin` — no more hardcoded
  `Access-Control-Allow-Origin: *`, though that stays the default for
  local dev) and **rate-limited admin routes** (`/v1/admin/*` now goes
  through the same per-client-IP token bucket as every other gateway
  route, not just the bearer-token check). Tests:
  `TestSetAllowedOriginRestrictsCORS` / `TestSetAllowedOriginEmptyRestoresWildcard`
  (`internal/api`), `TestAllowedOriginRestrictsCORS` /
  `TestEmptyAllowedOriginDefaultsToWildcard` (`internal/gateway`).
- `docker-compose.yml` and `deploy/docker/*.Dockerfile` already existed
  (non-root final-stage images, the real six-process topology) and now
  also thread `BOILERPULSE_CORS_ORIGIN` through every service, matching
  the existing `BOILERPULSE_ADMIN_TOKEN` pattern.
- `docs/deployment.md`: what's real, what's hardened this milestone, what
  real gaps remain (single shared admin secret, no TLS termination in this
  repo, no secrets-manager integration), and a concrete pre-flight
  checklist for anyone actually deploying this publicly.
- **What this milestone does not do**: provision an actual public host,
  domain, or TLS certificate, or spend anyone's money — those require
  credentials and a hosting decision that belong to this repo's owner, not
  something to do autonomously. See `docs/deployment.md` for exactly what
  remains.

Everything else in the repo tree (`internal/notifications`,
`tests/distributed`, `tests/end_to_end`) exists as a directory with a
README explaining what will live there and which milestone adds it — deliberately no
stub Go files with fake logic in them. (`internal/cluster` is reserved but its
responsibility is currently folded into `internal/gateway` — see that directory's README.)
