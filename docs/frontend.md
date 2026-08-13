# Frontend

The Next.js dashboard (`frontend/`) is BoilerPulse's observability
interface — every page reads real data from the gateway; nothing on the
site is simulated client-side.

## Pages

- **`/` Campus Dashboard** — system status (cluster mode + per-node
  reachability), current workload mode + RPS, the next 5 upcoming events,
  and an interactive traffic-prediction form (`POST /v1/predict`) — the
  spec's §31 "PREDICTED PEAK" element, backed by the real trained model
  rather than a mockup.
- **`/events` Event Feed** — every stored event (`GET /v1/events`), sorted
  by start time, with type/urgency/attendance/location.
- **`/cluster` Distributed Cluster** — the gateway's real cluster view
  (`GET /v1/cluster`): leader highlighted, each node's actual reachability
  and Raft term, polling every 2s.
- **`/metrics` Performance Metrics** — live RPS (a client-side rolling
  history built from polling `GET /v1/workload` every 2s — the backend
  only reports current RPS, not a time series, so the chart's x-axis is
  "since this page loaded," not historical), hot keys, and cache hit
  rate/evictions, via Recharts.
- **`/simulation` Traffic Simulator** — still an honest "not yet
  implemented" placeholder (Milestone 10's `cmd/simulator` doesn't exist
  yet); links to the dashboard's prediction form as the closest thing that
  *is* real today, while being clear it's a different feature (a single
  hypothetical-event query, not cluster load generation).
- **`/about` Architecture** — implemented/planned lists, kept in sync with
  `docs/roadmap.md`.

## Design notes

- `lib/api.ts` defaults to the **gateway** (`:8090`), not a single node —
  as of Milestone 4 the gateway is the intended entry point.
- `lib/usePoll.ts` is a generic polling hook (loading/error/ok states)
  used by every page — replaced an earlier node-specific
  `useClusterStatus` hook once its job generalized.
- Charts follow standard chart-quality practice: single axis, sequential
  single-hue for the RPS line (a magnitude, not a category), a bare bar
  chart for hot keys (one series needs no legend), a stat-tile grid for
  cache counters (a set of headline numbers, not a trend — not forced into
  a chart it doesn't need).
- No new heavy dependency for cluster topology — a plain CSS
  leader/followers layout, not a graph-visualization library, given three
  static nodes don't need one.

## Verifying

```bash
make cluster                              # backend: 3 nodes + gateway + ingest
cd frontend && npm install && npm run dev # frontend on :3000
```

Then open `http://localhost:3000` — the dashboard should show live cluster
status, workload mode, and (once `cmd/ingest` has posted a few) upcoming
events. Killing a node (or the whole cluster) should surface as a visible
error state on every page, not a silent blank screen — every page has an
explicit `status === "error"` branch.

This was actually done, not just described: every page was driven with a
headless-Chromium script (Playwright, installed temporarily for this and
removed afterward — not a project dependency) against a real running
cluster, checking `console --errors` on each page load in addition to
screenshotting it. That caught two real bugs `curl`-based testing and code
review both missed:

1. **CORS.** Neither the gateway nor a node sent
   `Access-Control-Allow-Origin`, so every fetch from the dashboard's
   origin (`:3000`) to the backend (`:8090`) was silently blocked by the
   browser — `curl` never enforces CORS, so this was invisible to every
   check up to that point. Fixed with a permissive CORS header in both
   `internal/gateway.Gateway.ServeHTTP` and `internal/api.Server.ServeHTTP`
   (including `OPTIONS` preflight handling), with tests asserting the
   headers are present.
2. **Unbounded event list.** After `cmd/ingest` had been running for a
   while during testing, `/events` had accumulated 300+ events and
   rendered every single one on one page — a ~22,000px-tall page. Fixed
   by capping the rendered list (50, soonest-first) with a "showing X of
   Y" note, found by actually loading the page after real accumulated
   state, not by reasoning about the data model in the abstract.
