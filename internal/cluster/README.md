# internal/cluster

Cluster membership, leader discovery, and node health used by the API gateway to route
writes to the leader and distribute reads.

This responsibility is implemented, but it lives in `internal/gateway` (Milestone 4)
rather than as a separate `internal/cluster` package — at the current scale (static,
config-driven node lists), splitting leader-discovery/health-polling out from the
gateway that's the only consumer of it would be premature separation. This directory
stays reserved in case that changes (e.g. dynamic membership, or a second consumer of
cluster state beyond the gateway) — see `docs/gateway.md` for what actually exists today.
