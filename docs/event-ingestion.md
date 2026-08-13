# Event Ingestion

`internal/events` implements the spec's normalized campus event model
(§6-9): a common `Event` shape, a pluggable `EventSource` interface, and the
validate → normalize → classify → estimate-traffic pipeline. `cmd/ingest`
runs it on a schedule; `internal/api` stores the result as ordinary KV data.

## Pipeline

```
EventSource.FetchEvents(ctx)
        │
        ▼
   RAW EVENT(S)
        │
        ▼
  events.Normalize()
        │  validate (title, known type, start_time, end<=start check, non-negative attendance)
        │  assign ID + CreatedAt if missing
        │  classify() -- fill Urgency if the source didn't set one
        │  estimateTraffic() -- fill ExpectedTrafficMultiplier if the source didn't set one
        │  confidenceFor() -- trust score (1.0 for SIMULATOR, lower otherwise)
        ▼
  POST /v1/events (cmd/ingest → gateway/node)
        │
        ▼
  internal/api: consistencyForUrgency() picks STRONG/EVENTUAL/CRITICAL,
  stores as an ordinary KV entry under "event:<id>"
```

Classification and traffic estimation only fill in zero values — a source
that already knows an event's urgency (e.g. a real emergency feed) isn't
overridden.

## Why events are just KV entries, not a separate store

Storing events under `event:<id>` keys in the same `storage.Engine` as
everything else is a deliberate choice, not a shortcut: the whole point of
this project is that the KV store is the real infrastructure and the campus
theme is its workload (see the top-level README). `GET /v1/events` is a
prefix `Scan("event:")` over the same engine — added to `storage.Engine` in
this milestone specifically for this — not a second storage system bolted
on the side.

## Urgency → consistency

`internal/api.consistencyForUrgency` ties event urgency directly to the
spec's workload-aware consistency model (§13):

| Urgency | Consistency |
|---|---|
| `CRITICAL` (EMERGENCY, WEATHER) | `CRITICAL` |
| `HIGH` / `SCHEDULED_SPIKE` | `STRONG` |
| `NORMAL` | `EVENTUAL` |

This is genuinely enforced today in the sense that the value is stored with
that tag — actual differentiated read/cache behavior *per* consistency
level (e.g. CRITICAL bypassing a cache) is Milestone 6's job.

## EventSource

```go
type EventSource interface {
    FetchEvents(ctx context.Context) ([]Event, error)
    Name() string
}
```

The only implementation is `SimulatorSource`: it picks randomly from a small
catalog of representative campus event templates (athletics, dining,
student org, academic, transportation, weather, emergency) and returns 1-3
per call. Per spec §40, external sources are optional adapters added later,
never a hard dependency — the system is designed to keep working with only
the simulator configured, which is also exactly what makes it useful for
generating a synthetic historical dataset later (Milestone 7).

## Trying it yourself

```bash
make cluster   # nodes + gateway + ingest, all in one command
tail -f .cluster/ingest.log     # watch synthetic events get generated and posted
curl localhost:8090/v1/events | jq   # see them, sorted by start time
make stop
```

Manually verified: ingest generates synthetic events every few seconds,
normalizes them (urgency classified, traffic multiplier estimated), posts
them through the gateway to the Raft leader, and they show up via
`GET /v1/events` on every node in the cluster — including nodes queried
directly, confirming they replicated through Raft like any other write.

See `docs/gateway.md` and `docs/raft.md` for the layers underneath this.
