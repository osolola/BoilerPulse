# Deployment

Milestone 11 (spec §57): what it actually takes to run BoilerPulse
somewhere other than a laptop, and what has to change first. There is no
live public instance of this project — provisioning one means choosing and
paying for a host, which is a decision for whoever owns this repo, not
something to do unilaterally. What's here is everything short of that:
real Dockerfiles, a real Compose topology, and a real hardening pass with
tests, so that decision is the only thing left.

## What's real here

- `deploy/docker/{node,gateway,ingest,frontend}.Dockerfile` — each service
  builds from `golang:1.24-alpine`/`node:22-alpine`, running as a non-root
  `boilerpulse` user (Go services) in a minimal `alpine:3.20` final stage.
- `docker-compose.yml` — the actual six-process topology (`kv-node-1/2/3`,
  `gateway`, `ingest`, `frontend`), matching what `make cluster` runs as
  local OS processes. Verified via `make cluster` and the automated
  integration/failure test suites; not yet run through Docker itself in
  this environment (Docker isn't installed here — see `docs/architecture.md`).
- Two real hardening changes made *for* this milestone, both with tests:
  configurable CORS origin and rate-limited admin routes (below).

## Hardening done this milestone

Two gaps were explicitly flagged as deferred in `docs/failure-testing.md`
("full auth hardening... before this goes near a public URL"). Both are
now fixed:

1. **CORS was hardcoded to `Access-Control-Allow-Origin: *`.** Fine for
   local dev (frontend and backend are both `localhost`, no untrusted
   origins exist), wrong for anything public — it means *any* website can
   make authenticated-looking requests against your cluster from a
   visitor's browser. `internal/api.Server.SetAllowedOrigin` and
   `internal/gateway.Options.AllowedOrigin` now let you restrict it to one
   real origin; `BOILERPULSE_CORS_ORIGIN` (env) / `cors_origin` (YAML) wire
   it through `cmd/node` and `cmd/gateway`. Still defaults to `*` — an
   empty value means "opt into the permissive dev default," not "silently
   block every request" — so nothing breaks for existing local setups, and
   `cmd/gateway` logs a warning if it starts without it set. Tests:
   `TestSetAllowedOriginRestrictsCORS` / `TestSetAllowedOriginEmptyRestoresWildcard`
   (`internal/api`), `TestAllowedOriginRestrictsCORS` /
   `TestEmptyAllowedOriginDefaultsToWildcard` (`internal/gateway`).
2. **Admin routes (`/v1/admin/*`) had no rate limit.** Every other gateway
   route was already behind the per-client-IP token bucket
   (`internal/gateway/ratelimit.go`); admin routes were only gated by the
   bearer token, with unlimited attempts. A leaked or brute-forced token
   now still gets throttled at the same per-IP rate as everything else —
   `g.rateLimited(g.authenticatedAdmin(...))` in `internal/gateway/gateway.go`.

## What's still a real gap (not fixed this milestone)

Documented here rather than hidden, matching every other "what's
simplified" section in this project's docs:

- **Single shared admin secret, not per-operator credentials.** One bearer
  token gates every kill/fault/restore call across every node. It can't be
  scoped to a specific operator, rotated without restarting every node
  that has it configured, or individually revoked. Fine for a solo
  demo/portfolio deployment; not fine for a team.
- **No TLS termination in this repo.** `docker-compose.yml` serves plain
  HTTP on every port. A real deployment needs a reverse proxy (nginx,
  Caddy, a cloud load balancer) doing TLS in front of the gateway and
  frontend — and *only* the gateway (`:8090`) and frontend (`:3000`)
  should ever be reachable from outside; the reverse proxy is also the
  right place to enforce that.
- **No secrets manager integration.** `BOILERPULSE_ADMIN_TOKEN` is a plain
  environment variable, sourced from `.env` locally. A real deployment
  should generate it randomly (`openssl rand -hex 32`, not a memorable
  string) and inject it via whatever secrets mechanism the host platform
  provides, never committed anywhere.
- **The write-throughput ceiling documented in `docs/benchmarking.md`.**
  Not a deployment-specific issue, but relevant to sizing: don't deploy
  this somewhere expecting to sustain much above ~50-80 writes/sec without
  first addressing the WAL group-commit gap described there.

## Before exposing this publicly: a checklist

1. Set `BOILERPULSE_ADMIN_TOKEN` to a long random value (`openssl rand
   -hex 32`), the same value on every node and the gateway. Never commit
   it; never reuse the local-dev `dev-chaos-token` default `make cluster`
   uses.
2. Set `BOILERPULSE_CORS_ORIGIN` (env) on every node and the gateway to
   the frontend's real origin (e.g. `https://boilerpulse.example.com`) —
   not `*`.
3. Set `NEXT_PUBLIC_API_URL` (frontend) to the gateway's real public URL.
4. Put a reverse proxy in front of the gateway and frontend doing TLS;
   don't expose `:8090`/`:3000` directly, and never expose the per-node
   Raft ports (`:9080-9082`), admin ports (`:7080-7082`), or KV HTTP ports
   (`:8080-8082`) at all — only the gateway and frontend are meant to be
   public.
5. Consider whether the admin/chaos endpoints belong on a public instance
   at all — they're real infrastructure controls (kill a node, drop
   packets). If they stay reachable, make sure step 1's token is treated
   with the same care as any other production credential.
6. Re-read `docs/benchmarking.md`'s sizing note before pointing real
   traffic at it.

## Trying the Docker topology

```bash
cp .env.example .env
# edit .env: set BOILERPULSE_ADMIN_TOKEN to something real
docker compose build
docker compose up
```

`docker-compose.yml` reads both `BOILERPULSE_ADMIN_TOKEN` (defaults to
empty, which disables every admin endpoint — see
`docs/failure-testing.md`) and `BOILERPULSE_CORS_ORIGIN` (defaults to
empty, meaning `*`) from the environment for every `kv-node-*` and the
`gateway`. Set both before deploying anywhere the frontend's real origin
differs from `localhost`.

See `docs/architecture.md` for the full system design,
`docs/failure-testing.md` for the admin/chaos surface this hardens, and
`docs/benchmarking.md` for real capacity numbers to size against.
