# Workload Model

`internal/workload` implements the spec's automatic workload-mode state
machine (§26) and hot-key detection (§12); `internal/cache` implements the
adaptive LRU cache (§25). `internal/gateway` is what feeds them real traffic
and enforces the caching rules — it's the only component that sees every
request, so it's the natural place for this.

## Workload modes

```
NORMAL → ElevatedRPS crossed → ELEVATED → HighTrafficRPS crossed → HIGH_TRAFFIC
                                                                          │
                                                          CriticalRPS crossed, OR
                                                     a CRITICAL-urgency event posted
                                                                          ▼
                                                                      CRITICAL
```

Mode selection is purely reactive to the current requests-per-second
average over a 10s sliding window — recomputed on every request, with no
hysteresis or cooldown on the way back down. That's a deliberate
simplification (see below), except for one case: an explicit
`SignalCritical` call (triggered when the gateway sees a POST /v1/events
response with `"urgency":"CRITICAL"`) holds CRITICAL mode for a minimum
duration regardless of request rate — matching the spec's "Emergency events
can directly enter CRITICAL."

The gateway reads the **response** `internal/api` sends back for a created
event, not the client's request body, specifically because a client (like
`cmd/ingest`'s `SimulatorSource`) often doesn't set `urgency` itself —
`internal/events.Normalize` classifies it server-side. Reading the response
means this works correctly for every EMERGENCY/WEATHER event the simulator
generates, not just ones a caller explicitly marks CRITICAL.

## Hot-key detection

`HotKeyTracker` counts requests per KV key over the same 10s window and
reports any key at or above a threshold (20 requests by default), sorted by
count descending. It's fed from `kvKeyFromPath` extracting the key out of
every `/v1/kv/{key}` request the gateway proxies — reads and writes both
count.

Both `RequestMonitor` and `HotKeyTracker` bucket by **millisecond**, not
whole seconds — an early version used `time.Now().Unix()` bucket keys, which
meant `int64(window.Seconds())` truncated any sub-second window's cutoff to
`0`, silently breaking the slide. Tests using short windows for speed caught
this immediately; production's 10s window was never actually affected, but
millisecond buckets make the math correct at any timescale rather than
correct-by-coincidence at the one timescale that happened to be tested.

## Adaptive caching

`internal/cache.LRU` is a standard capacity-bounded LRU
(`container/list` + a map), wired into the gateway's read path:

- `GET /v1/kv/{key}` checks the cache first; a hit skips proxying to a node
  entirely (`X-Cache: HIT` header) and doesn't count toward round-robin
  read distribution.
- On a cache miss, the gateway proxies to a node as usual, and caches the
  response **only if its `consistency` field is not `"CRITICAL"`** — the
  spec is explicit that emergency data must never be served stale from a
  cache (§25), and this is checked on every single cached value, not just
  configured per-key.
- A `PUT`/`DELETE` to a key invalidates that key's cache entry immediately
  (correctness over cache-hit-rate: better to take a miss than serve a
  value that's already been overwritten).
- Only `/v1/kv/{key}` responses are cached — `/v1/events` (a list) is
  deliberately excluded, since a cached list could contain a since-changed
  CRITICAL event and the single-entry consistency check doesn't apply
  cleanly to a list.

## What's simplified

- **No hysteresis on mode transitions.** A workload mode can flap between
  NORMAL and ELEVATED if request rate oscillates near a threshold. A real
  system would debounce this (e.g. require N consecutive above-threshold
  samples before escalating, or a cooldown before de-escalating).
- **No "increase read replicas" or dynamic capacity response to hot keys**
  (spec §12's "increase read replicas" step) — cluster membership is still
  static (see `docs/raft.md`); this is explicitly deferred to whatever
  proactive-scaling work happens later.
- **Caching is gateway-only**, not per-node — a client talking directly to
  a node (bypassing the gateway) gets no caching at all. This matches the
  project's own layering: the gateway is the one component with a global
  view of traffic, so it's the natural cache boundary.
- Only KV consistency (`STRONG`/`EVENTUAL`/`CRITICAL`) gates caching today;
  the full read-routing implications of that consistency model (e.g.
  STRONG reads always hitting the leader) aren't enforced anywhere yet.

## Trying it yourself

```bash
make cluster
curl -X PUT localhost:8090/v1/kv/hot -d '{"value":"v","consistency":"EVENTUAL"}'
curl -D - -o /dev/null localhost:8090/v1/kv/hot   # X-Cache: (absent, miss)
curl -D - -o /dev/null localhost:8090/v1/kv/hot   # X-Cache: HIT

for i in $(seq 1 25); do curl -s -o /dev/null localhost:8090/v1/kv/hot; done
curl localhost:8090/v1/workload   # mode, rps, hot_keys, cache stats

curl -X POST localhost:8090/v1/events \
  -d '{"type":"EMERGENCY","title":"Test","start_time":"2026-01-01T00:00:00Z"}'
curl localhost:8090/v1/workload   # mode: CRITICAL

make stop
```

Manually verified: cache hit/miss behavior, cache invalidation on write,
hot-key detection after a request burst, and CRITICAL-event-triggers-
CRITICAL-mode, all against a real 3-node cluster + gateway — including
confirming a CRITICAL-consistency key is genuinely never cached (`X-Cache`
never present across repeated GETs).
