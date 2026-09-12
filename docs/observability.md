# Observability

Every `cmd/node` and `cmd/gateway` process exposes `GET /metrics` in
Prometheus exposition format (`internal/metrics`). This is real
instrumentation wired into the actual request path and the actual Raft/
storage/cache internals — not a stub endpoint that returns a fixed sample
payload.

## What's exposed

- **HTTP** (`boilerpulse_http_*`): request count and latency histogram,
  labelled by method, matched route *pattern*, and status class.
- **Raft** (`boilerpulse_raft_*`, node only): current term, whether this
  node is leader, commit index, last-applied index, last log index, and
  `apply_lag` (commit index minus last-applied — a sustained nonzero value
  means the state machine is falling behind consensus).
- **Storage** (`boilerpulse_storage_*`, node only): memtable size in bytes
  and entry count, SSTable count.
- **Cache** (`boilerpulse_cache_*`, gateway only): cumulative hits, misses,
  evictions.
- **Workload** (`boilerpulse_workload_*`, gateway only): current RPS, and
  one `mode` gauge per workload mode (NORMAL/ELEVATED/HIGH_TRAFFIC/CRITICAL),
  1 for whichever is active and 0 for the rest.
- Standard Go/process collectors (goroutines, GC, memory, file descriptors)
  from `prometheus/client_golang/collectors`, the same ones any Go service
  exposes.

## Two design decisions worth knowing

**Route pattern, not raw path, is the HTTP label.** The label is
`r.Pattern` (e.g. `PUT /v1/kv/{key}`), populated by Go's `net/http.ServeMux`
once a request has matched. Labelling by the raw path instead would create
one time series per key ever written — for a KV store, unbounded
cardinality, and the standard way to degrade or crash a Prometheus server.
`TestMiddlewareRecordsRouteNotRawPath` asserts the raw key never leaks into
a label.

**Everything except HTTP counters is pull-based.** Raft/storage/cache/
workload gauges are backed by callback functions (`GaugeFunc`) that run at
scrape time, registered once via `RegisterRaftSource` /
`RegisterStorageSource` / `RegisterCacheSource` / `RegisterWorkloadSource`.
There's no background goroutine polling internals and caching a copy — a
scrape always reads live state, and there's one less concurrent thing that
can drift or leak.

Each process also gets its own `prometheus.Registry` rather than the global
default one (`internal/metrics.New` builds a fresh registry per call). The
global default registerer panics on duplicate registration, which matters
for tests that construct several `*Metrics` in one process.

## What's simplified

- **No alerting rules or a bundled Prometheus/Grafana stack.** This exposes
  the `/metrics` endpoint; scraping it, storing it, and alerting on it is
  left to whatever the deployer already runs. `docs/deployment.md` doesn't
  currently include a Prometheus service in `docker-compose.yml` — adding
  one (plus a starter Grafana dashboard) is reasonable future work.
- **No per-key or per-consistency-tier metrics.** Deliberately, per the
  cardinality point above — `boilerpulse_cache_hits_total` is a single
  counter, not broken out by key.
- **`/metrics` is unauthenticated**, same as every other read-only endpoint
  in this project today. Exposes operational detail (Raft term, cache hit
  rate) but not KV data itself. Worth gating behind the reverse proxy
  `docs/deployment.md` already recommends before a public deployment.

## Trying it yourself

```bash
make cluster
curl localhost:8080/metrics | grep boilerpulse_raft   # node-1's Raft state
curl localhost:8090/metrics | grep boilerpulse_cache   # gateway's cache stats

# watch the leader's commit index move under load
./bin/simulator -scenario normal -target http://localhost:8090 -topology 3-node &
watch -n1 'curl -s localhost:8080/metrics | grep boilerpulse_raft_commit_index'

make stop
```

Manually verified against a real 3-node cluster: `boilerpulse_raft_term`,
`is_leader`, and `commit_index` all reflect real election/replication state
(confirmed against `GET /v1/cluster`'s output on the same node); a real
`PUT` through the gateway immediately incremented
`boilerpulse_http_requests_total{route="PUT /v1/kv/{key}"}` and populated
its latency histogram; `boilerpulse_storage_memtable_bytes` moved with real
writes.
