const IMPLEMENTED = [
  "Durable storage engine: WAL (checksummed, fsync per write), memtable, SSTables, compaction",
  "Crash recovery: kill -9 -> restart -> data survives (WAL replay + atomic SSTable flush)",
  "Hand-rolled Raft: leader election, log replication, failover, crash-safe persistence, real gRPC transport",
  "API gateway: leader routing, read distribution, transparent failover, rate limiting, real cluster status",
  "Campus event model + SimulatorSource ingestion, normalized and replicated through Raft",
  "Workload modes (NORMAL -> CRITICAL) driven by live traffic; hot-key detection; adaptive LRU cache that never caches CRITICAL data",
  "Traffic prediction: linear regression trained from scratch on documented synthetic data (POST /v1/predict)",
  "KV HTTP API: PUT/GET/DELETE /v1/kv/{key}, GET/POST /v1/events, GET /v1/cluster, GET /v1/workload, GET /healthz",
  "Chaos/failure injection: per-node kill/latency/packet-drop/partition, a CLI (scripts/chaos.sh), and live controls on /cluster",
  "Real, measured benchmarks (cmd/simulator) across five traffic scenarios, single-node and 3-node — see /simulation",
  "Deployment hardening: configurable CORS origin, rate-limited admin routes, real Dockerfiles + Compose topology",
  "This dashboard, wired to all of the above — no simulated data anywhere on this site",
  "Structured JSON logging, YAML + env config, unit + integration tests, race-detector clean throughout",
];

const PLANNED = [
  "An actual public URL — everything short of provisioning a host is done (docs/deployment.md); hosting is a decision for the repo owner, not something done automatically",
];

export default function AboutPage() {
  return (
    <main className="mx-auto max-w-3xl px-6 py-10">
      <h1 className="text-2xl font-semibold tracking-tight">Architecture</h1>
      <p className="mt-2 text-sm text-black/60 dark:text-white/60">
        BoilerPulse is a distributed, Raft-replicated, LSM-tree key-value store, wrapped
        around a campus-events workload used to exercise it. The campus theme is the
        workload generator, not the point of the project — see the repo README and{" "}
        <code>docs/architecture.md</code> for the full design.
      </p>

      <h2 className="mt-8 text-lg font-medium">Implemented today</h2>
      <ul className="mt-3 list-disc space-y-1 pl-5 text-sm">
        {IMPLEMENTED.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>

      <h2 className="mt-8 text-lg font-medium">Planned</h2>
      <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-black/70 dark:text-white/70">
        {PLANNED.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </main>
  );
}
