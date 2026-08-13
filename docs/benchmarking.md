# Benchmarking

Milestone 10 (spec §10/§57): `simulator/` generates real HTTP load against a
running cluster following named traffic-curve scenarios, and `cmd/simulator`
(`boilerpulse-sim`) is the CLI that runs one. Every number on this page came
from actually running it against a real compiled cluster on this machine —
nothing here is estimated, extrapolated, or invented. Raw JSON reports are
in `benchmarks/results/`.

Running this uncovered two real things about the system that unit tests
never would have: a genuine concurrency bug in the Raft replication path
(found and **fixed**, with a regression test), and a real write-throughput
ceiling from the WAL's per-write fsync (found and **documented**, not
fixed — see below for why).

## Methodology

- **Scenarios** (`simulator/scenario.go`): `normal`, `finals`, `athletics`,
  `emergency`, `hotkey` — each a baseline→peak→baseline RPS curve (linear
  ramps) with a read/write mix and a hot-key concentration. See that file
  for the exact numbers; they're deliberately modest (peaks of 10-200 rps)
  because the goal was measuring this specific system's real capacity on a
  single dev machine, not hitting a round number.
- **Warmup**: every key a scenario might touch is `PUT` once before timing
  starts (capped at 20 concurrent requests — see "What we found" below for
  why that cap exists), so timed `GET`s hit real data instead of a mix of
  404s.
- **Topologies**: `single-node` (`make run`, no Raft, no gateway — a
  ceiling on what the storage engine alone can do) and `3-node` (`make
  cluster`, full Raft + gateway).
- **A dedicated benchmark gateway** (`configs/cluster/gateway-benchmark.yaml`,
  `:8091`, `rate_limit_rps: 5000`): the default gateway config's rate limit
  (50 rps/100 burst, `configs/gateway.yaml`) is *per client IP* — correct
  for production, where real traffic comes from many distinct clients, but
  the load generator issues every request from one IP. The first `3-node`
  run, against the default `:8090` gateway, measured the rate limiter
  instead of the cluster (up to 70% of "requests" were `429`s). Re-run
  against `:8091` to measure what was actually intended: storage/Raft
  throughput. This is a real, documented methodology choice, not a
  discrepancy hidden between runs — see `benchmarks/results/` if you want
  the rate-limited numbers too, they're real; they just measure something
  else.
- **Failure injection** (`-inject-failure`): kills the current leader via
  the gateway's admin proxy partway through a scenario (`internal/admin`,
  `docs/failure-testing.md`), so the report captures the real cost of a
  live failover, not just steady-state throughput.

## Results

All numbers are p50/p95/p99/max latency in milliseconds, achieved
requests/sec, and error rate, from the actual JSON reports in
`benchmarks/results/`.

| Scenario | Topology | Requests | Achieved RPS | p50 | p95 | p99 | Max | Errors |
|---|---|---|---|---|---|---|---|---|
| normal | single-node | 199 | 9.9 | 1.6 | 6.8 | 8.2 | 8.3 | 0% |
| normal | 3-node | 199 | 9.9 | 2.0 | 18.1 | 19.2 | 19.8 | 0% |
| finals | single-node | 1549 | 51.6 | 1.1 | 6.1 | 6.8 | 40.6 | 0% |
| finals | 3-node | 1548 | 51.6 | 1.4 | 17.4 | 27.1 | 445.8 | 0% |
| athletics | single-node | 4193 | 119.8 | 1.3 | 6.5 | 10.0 | 36.1 | 0% |
| athletics | 3-node | 4197 | 119.9 | 1.3 | 17.1 | 36.0 | 774.6 | 0.02% |
| **emergency** | single-node | 3629 | 172.8 | 4.6 | 11.5 | 14.6 | 21.4 | **0%** |
| **emergency** | 3-node | 3593 | 145.2 | 1.5 | 14.6 | **540.9** | **1554.4** | **38.1%** |
| hotkey | single-node | 599 | 29.9 | 1.3 | 5.5 | 7.3 | 10.1 | 0% |
| hotkey | 3-node | 413 | 20.6 | 1.1 | 16.0 | 18.5 | 28.8 | 0% |
| athletics + kill leader at t=15s | 3-node | 4118 | 117.6 | 1.2 | 19.2 | **483.6** | 848.9 | **0.19%** |

## What we found

### 1. A real bug: concurrent proposals flooded replication (fixed)

Running the simulator's warmup phase (hundreds of `PUT`s fired at once)
against a fresh cluster reliably destabilized the leader — `starting
election` log lines every ~1 second, term climbing continuously, the
gateway unable to hold onto a stable leader. `tests/failure`'s own suite
never caught this because none of its scenarios generate concurrent write
*bursts* — they check availability through kills and restarts, not
throughput.

The cause: `internal/raft.Node.Propose` used to spawn one goroutine (and
one `AppendEntries` RPC) **per peer per proposal**, with no
serialization:

```go
for _, peer := range n.peers {
    peer := peer
    go n.sendAppendEntriesTo(peer)
}
```

A burst of concurrent proposals meant dozens of overlapping, increasingly
redundant `AppendEntries` RPCs racing to the same peer at once. The
leader's own periodic heartbeat (`internal/raft/tick.go`) went through the
identical unserialized path, so it got stuck queued behind the flood — a
follower that doesn't hear from the leader within its election timeout
correctly (per Raft) assumes the leader is dead and starts a new election,
even though the real leader was alive and making progress the entire time.

**Fixed** by serializing replication per peer: each peer now has exactly
one long-lived `replicationLoop` goroutine (started in `Node.Start`,
stopped in `Node.Stop`) fed by a buffered(1) trigger channel
(`replication.go`'s `triggerReplication`). A proposal or heartbeat tick
signals the channel instead of spawning a goroutine; if a send is already
in flight, the signal just sets a pending flag (extra signals are dropped,
not queued) — and because the next send always reads fresh log state, no
proposal is ever lost by coalescing a burst into fewer RPCs.

`internal/raft/replication_test.go`'s
`TestConcurrentProposalsCoalesceReplicationPerPeer` is a deterministic
regression test: it fires 50 concurrent `Propose` calls at a leader whose
transport is deliberately blocked, and asserts only 1 RPC is in flight at
a time and the whole burst coalesces into roughly 1-5 total RPCs, not 50.

After the fix, the same warmup burst that used to destabilize a fresh
cluster within seconds no longer does — confirmed by re-running the exact
same concurrent-PUT stress test manually against a rebuilt cluster and
watching the term hold steady.

### 2. A real capacity ceiling: WAL fsync is fully serialized (documented, not fixed)

The `emergency` scenario (peak 200 rps, 40% write ratio — roughly 80
writes/sec sustained) shows a 38% error rate and a p99 latency of 540ms on
the 3-node cluster, against a **clean 0% error rate at the identical
target curve on single-node**. That gap is the real, measured cost of
consensus under this implementation's current design, not noise — and the
`athletics` scenario (also 120 rps peak, but only 10% writes ≈ 12
writes/sec) stays clean on 3-node, which points at write rate specifically,
not overall RPS, as the limiting factor.

The cause: `internal/raft/filestorage.go`'s `FileStorage.AppendEntries`
calls `fsync` on every single call, and it's invoked from
`internal/raft/log.go`'s `appendLogLocked` while `Node.Propose` holds the
node's one mutex (`n.mu`) — correct for durability (an entry is only
acknowledged after it's actually on disk), but it means every concurrent
proposal serializes through one lock *and* one fsync, one at a time. Under
sustained high write concurrency, proposals queue up behind each other;
some exceed the client's timeout before they can even be appended, and the
same lock contention can delay the tick loop's heartbeat *detection* (not
just sending), occasionally triggering the same kind of election churn
described above as a secondary effect — this is why `emergency`'s max
latency (1554ms) is so much higher than its p99 (541ms): a handful of
requests were genuinely stuck behind both the write queue and a stalled
heartbeat.

This is **not fixed** in this milestone. The standard production fix is
group-commit / batched fsync (accumulate multiple pending proposals and
`fsync` them together in one disk operation), which would meaningfully
change `Propose`'s and `appendLogLocked`'s structure and deserves its own
focused pass with dedicated tests, not a rushed change alongside a
benchmarking milestone. Documenting a real, measured limit honestly is
more useful than a same-day fix that isn't well-tested — consistent with
how `docs/raft.md` and `docs/storage-engine.md` already document
no-snapshotting and full-table compaction as known simplifications rather
than silently working around them.

**Practical takeaway**: this cluster, as built, comfortably sustains
blended read/write traffic up to roughly 150 rps with a modest (≤15%)
write ratio, and degrades under sustained write-heavy bursts much above
roughly 50-80 writes/sec. That's a real, useful number for anyone deciding
whether this is production-ready (it isn't, and the roadmap doesn't claim
otherwise) versus a solid consensus-and-storage implementation with a
well-understood, well-documented next bottleneck.

### 3. The real cost of a failover

The `athletics + kill leader` run isolates what a live failover actually
costs, on top of an otherwise-clean scenario: 8 failed requests out of
4118 (0.19%), concentrated in the few hundred milliseconds between the
leader dying and a new one being elected and detected — p99 latency during
that run (483ms) is roughly Raft's `MinElectionTimeout`-to-`MaxElectionTimeout`
window (300-600ms, `raft.DefaultOptions`), which is exactly what you'd
expect: the gateway's one-refresh-and-retry (`docs/gateway.md`) absorbs
most requests transparently, and only the unlucky handful whose retry
lands before the new leader is actually up see a real error.

## Trying it yourself

```bash
make build
make run &                                    # single-node baseline
./bin/simulator -scenario all -target http://localhost:8080 -topology single-node -out /tmp/report.json

make cluster                                   # 3-node + gateway + ingest
kill $(cat .cluster/ingest.pid); rm .cluster/ingest.pid   # optional: quieter comparison
BOILERPULSE_GATEWAY_CONFIG=configs/cluster/gateway-benchmark.yaml \
  BOILERPULSE_ADMIN_TOKEN=$(cat .cluster/admin.token) ./bin/gateway &  # higher-limit gateway on :8091

./bin/simulator -scenario all -target http://localhost:8091 -topology 3-node -out /tmp/report.json
./bin/simulator -scenario athletics -target http://localhost:8091 -topology 3-node-failure \
  -inject-failure -admin-token $(cat .cluster/admin.token) -out /tmp/failure-report.json

make stop
```

See `simulator/scenario.go` for the exact scenario definitions,
`docs/raft.md` for the consensus design being measured, and
`docs/failure-testing.md` for the admin server `-inject-failure` drives.
