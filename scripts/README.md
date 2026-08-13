# scripts/

- `chaos.sh` — CLI wrapper for the chaos/failure-injection admin server
  (`internal/admin`, spec §23/§34): kill, restart, partition, latency,
  packet-drop, and restore, per node. Talks directly to each node's admin
  port rather than through the gateway. See `docs/failure-testing.md`.
  Requires `make cluster` to be running (or `BOILERPULSE_ADMIN_TOKEN` set
  manually). Run `scripts/chaos.sh` with no arguments for usage.

`dev.sh`, `benchmark.sh` — not yet implemented; `benchmark.sh` arrives with
Milestone 10.
