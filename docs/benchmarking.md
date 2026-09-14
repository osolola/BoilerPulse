# Benchmarking

Milestone 10 (spec §10/§57): `simulator/` generates real HTTP load against a
running cluster following named traffic-curve scenarios, and `cmd/simulator`
(`boilerpulse-sim`) is the CLI that runs one. Every number on this page came
from actually running it against a real compiled cluster on this machine —
nothing here is estimated, extrapolated, or invented. Raw JSON reports are
in `benchmarks/results/`.

Running this uncovered real things about the system unit tests never
would have: a genuine concurrency bug in the Raft replication path (found
and **fixed**, with a regression test), a write-throughput ceiling from
the WAL's per-write fsync (found, then **fixed** via group-commit
batching — see below), and, once that fix increased the commit rate
enough to expose it, a second, deeper bottleneck in the sequential
state-machine apply path (found and **documented**, not yet fixed — same
section).

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

**Current numbers** (after the group-commit work below; raw JSON in
`benchmarks/results/`):

| Scenario | Topology | Requests | Achieved RPS | p50 | p95 | p99 | Max | Errors |
|---|---|---|---|---|---|---|---|---|
| normal | single-node | 199 | 9.9 | 0.7 | 6.2 | 8.0 | 9.0 | 0% |
| normal | 3-node | 199 | 9.9 | 1.5 | 17.1 | 20.1 | 112.6 | 0% |
| finals | single-node | 1547 | 51.6 | 0.7 | 6.0 | 13.0 | 142.6 | 0% |
| finals | 3-node | 1549 | 51.6 | 1.2 | 16.5 | 20.5 | 35.2 | 0% |
| athletics | single-node | 4167 | 119.1 | 0.9 | 5.9 | 13.7 | 286.6 | 0% |
| athletics | 3-node | 4189 | 119.7 | 0.8 | 16.2 | 26.8 | 50.0 | 0% |
| **emergency** | single-node | 3609 | 171.8 | 1.1 | 9.6 | 14.5 | 135.4 | **0%** |
| **emergency** | 3-node | 3629 | 117.8 | 1.2 | 290.4 | **1423.0** | **1955.2** | **36.2%** |
| hotkey | single-node | 598 | 29.9 | 1.1 | 5.1 | 6.9 | 9.7 | 0% |
| hotkey | 3-node | 599 | 29.9 | 1.0 | 15.6 | 18.6 | 26.4 | 0% |
| athletics + kill leader at t=15s | 3-node | 4163 | 118.9 | 1.0 | 16.4 | **29.5** | 145.1 | **0.22%** |

**Before the group-commit work** (the numbers Milestone 10 originally
shipped with, for comparison — the "What we found" section below explains
exactly what changed and why the last row didn't):

| Scenario | Topology | p99 | Max | Errors |
|---|---|---|---|---|
| finals | 3-node | 27.1 | 445.8 | 0% |
| athletics | 3-node | 36.0 | 774.6 | 0.02% |
| **emergency** | 3-node | **540.9** | **1554.4** | **38.1%** |
| hotkey | 3-node | 18.5 | 28.8 | 0% |
| athletics + kill leader at t=15s | 3-node | **483.6** | 848.9 | **0.19%** |

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

### 2. The WAL fsync ceiling — fixed at the propose layer, which uncovered a second, deeper bottleneck

The original finding here (Milestone 10) was that `FileStorage.AppendEntries`
fsyncs on every call, invoked from `Node.Propose` while holding the node's
one mutex — so every concurrent proposal serialized through one lock *and*
one fsync, one at a time. That's now fixed: `Propose` enqueues onto a
channel (`apply.go`'s `appendLoop`), and a single goroutine drains
whatever has piled up and appends the whole batch with **one** fsync
(`internal/raft/apply.go`, `internal/storage/lsm/engine.go`'s analogous
`opLoop` for direct single-node writes, `internal/storage/wal/writer.go`'s
new `AppendBatch`). `TestConcurrentProposalsCoalesceIntoFewerAppendCalls`
and `TestConcurrentPutsCoalesceIntoFewerWALBatches` prove this directly —
50 concurrent writers collapse into single-digit fsyncs, not 50.

The fix genuinely worked for every scenario except one: `finals`,
`athletics`, and `hotkey` all hold at 0% errors with **better** tail
latency than before (`athletics`'s p99 dropped from 36.0ms to 26.8ms, its
max from 774.6ms to 50.0ms), and the failover scenario's p99 dropped from
484ms to **29.5ms** — a new leader can now catch up a backlog of pending
proposals in one batch instead of replaying them one fsync at a time.

**`emergency` did not improve — it's still ~36% errors, and its p99
actually got worse (541ms → 1423ms).** Chasing why exposed a real, deeper
bottleneck the propose-side fix could never have touched:

`internal/metrics`'s `boilerpulse_raft_apply_lag` gauge (Milestone
post-11's Day 1) made this directly observable. Polling it live during an
`emergency` run:

```
t=1s  commit_index=5034  apply_lag=18
t=9s  commit_index=5530  apply_lag=12
t=10s commit_index=5619  apply_lag=104
t=15s commit_index=6180  apply_lag=514
t=20s commit_index=6701  apply_lag=876
t=30s commit_index=7840  apply_lag=1393
```

The Raft log itself is no longer the bottleneck — `commit_index` climbs at
a healthy, steady ~100/sec throughout, proving the propose-side fix is
doing its job. But `lastApplied` falls further behind every second,
unboundedly. The reason: `internal/raft/apply.go`'s `applyPending` applies
committed entries to the state machine **strictly one at a time** — it
calls `stateMachine.Apply` (which calls `engine.Put`, which goes through
the very `opLoop` group-commit machinery built for this milestone) and
waits for that call to fully return before even considering the next
entry. There is structurally never more than one `Put` in flight from this
path, so `opLoop` never sees more than a batch of one — **the apply path
gets zero benefit from group commit, no matter how well-batched the
propose path is.** Speeding up commits without speeding up applies just
moves the queue from one end of the pipeline to the other: `Propose`
blocks in `waitForApply` until `lastApplied` reaches its index, so a
growing apply lag shows up to a client as growing latency, and past ~5s
(the load generator's client timeout), as an outright error — which is
exactly the error and p99 numbers above.

This is **found and documented, not fixed**, same as the original finding
one layer up. A real fix means batching the *apply* path too — the leader
already knows exactly how many entries are ready to apply at once
(`commitIndex - lastApplied`), so `applyPending` could hand a whole batch
to the state machine at once instead of one command at a time. Doing that
safely needs a real design decision this milestone didn't have time for:
whether `StateMachine` gains a proper `ApplyBatch` method (a real interface
change, needing the same care Day 2's `Snapshot`/`Restore` addition got),
or whether entries get parallelized some other way that doesn't risk
applying two updates to the same key out of order — the exact race
`TestConcurrentPutsToSameKeyApplyInSeqOrder` (`internal/storage/lsm`) exists
to guard against for the *propose* side, and would need an equivalent
guarantee on the *apply* side before this can be done safely. Documenting
a real, measured limit — and exactly why the obvious next fix isn't a
same-day change — is more useful than pretending the story ends here.

**Practical takeaway**: this cluster now comfortably sustains everything
tested up to and including a 150 rps blended load, and a real leader
failure during load now costs single-digit milliseconds of p99 instead of
hundreds. Only the most extreme sustained write-heavy scenario (`emergency`,
~80 writes/sec peak, 90% concentrated on one key) still saturates — for a
different, now-precisely-identified reason than originally diagnosed. Any
future work on this should start from `boilerpulse_raft_apply_lag`, not
from re-deriving the diagnosis from scratch.

### 3. The real cost of a failover

The `athletics + kill leader` run isolates what a live failover actually
costs, on top of an otherwise-clean scenario: 9 failed requests out of
4163 (0.22%), concentrated in the window between the leader dying and a
new one being elected and detected — the gateway's one-refresh-and-retry
(`docs/gateway.md`) absorbs most requests transparently, and only the
unlucky handful whose retry lands before the new leader is actually up see
a real error. p99 latency during that run dropped from 484ms (before the
group-commit fix above) to **29.5ms** — a new leader now catches up
whatever backlog of pending proposals accumulated during the election in
one batched append instead of replaying it one fsync at a time, which
turns out to matter more for failover recovery time than for steady-state
throughput.

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

Watch `boilerpulse_raft_apply_lag` (`docs/observability.md`) on the leader
while a scenario runs to see the apply-path bottleneck described above
directly: `watch -n1 'curl -s localhost:8081/metrics | grep raft_apply_lag'`
(adjust the port to whichever node is leader).

See `simulator/scenario.go` for the exact scenario definitions,
`docs/raft.md` for the consensus design being measured, and
`docs/failure-testing.md` for the admin server `-inject-failure` drives.
