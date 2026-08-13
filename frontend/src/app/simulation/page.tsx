import Link from "next/link";
import { BENCHMARK_RESULTS, RECORDED_AT, type BenchmarkResult } from "./benchmark-results";

export default function SimulationPage() {
  const byScenario = new Map<string, BenchmarkResult[]>();
  for (const r of BENCHMARK_RESULTS) {
    const list = byScenario.get(r.scenario) ?? [];
    list.push(r);
    byScenario.set(r.scenario, list);
  }

  return (
    <main className="mx-auto max-w-5xl px-6 py-10">
      <h1 className="text-2xl font-semibold tracking-tight">Traffic Simulator</h1>
      <p className="mt-1 text-sm text-black/60 dark:text-white/60">
        Real recorded benchmark runs (Milestone 10) from <code>cmd/simulator</code> (
        <code>boilerpulse-sim</code>) against a compiled single-node and 3-node cluster on{" "}
        {RECORDED_AT} — not live, not estimated. See{" "}
        <span className="font-mono">docs/benchmarking.md</span> for full methodology,
        including a real Raft replication bug this found and fixed, and a real
        write-throughput ceiling it found and documented.
      </p>

      <div className="mt-8 space-y-8">
        {Array.from(byScenario.entries()).map(([scenario, rows]) => (
          <ScenarioTable key={scenario} scenario={scenario} rows={rows} />
        ))}
      </div>

      <div className="mt-10 rounded-lg border border-black/10 p-4 text-sm dark:border-white/10">
        <div className="font-medium">Run it yourself</div>
        <p className="mt-1 text-black/60 dark:text-white/60">
          This page shows one recorded run per scenario/topology — it doesn&apos;t generate
          live load from the browser (a public demo shouldn&apos;t have a button that can
          hammer its own backend). Use the CLI against a real running cluster instead:
        </p>
        <pre className="mt-3 overflow-x-auto rounded bg-black/[.04] p-3 text-xs dark:bg-white/[.06]">
          {`make cluster
./bin/simulator -scenario all -target http://localhost:8090 -topology 3-node -out /tmp/report.json`}
        </pre>
      </div>

      <p className="mt-6 text-sm text-black/60 dark:text-white/60">
        Not the same thing, but already real: the{" "}
        <Link href="/" className="underline">
          dashboard&apos;s traffic prediction form
        </Link>{" "}
        queries the actual trained model (<code>POST /v1/predict</code>) for a single
        hypothetical event — it doesn&apos;t generate load against the cluster.
      </p>
    </main>
  );
}

function ScenarioTable({ scenario, rows }: { scenario: string; rows: BenchmarkResult[] }) {
  return (
    <section>
      <h2 className="text-sm font-medium capitalize">{scenario}</h2>
      <div className="mt-2 overflow-x-auto rounded-lg border border-black/10 dark:border-white/10">
        <table className="w-full min-w-[640px] text-left text-xs">
          <thead className="border-b border-black/10 text-black/50 dark:border-white/10 dark:text-white/50">
            <tr>
              <th className="px-3 py-2 font-normal">Topology</th>
              <th className="px-3 py-2 font-normal">Requests</th>
              <th className="px-3 py-2 font-normal">Achieved RPS</th>
              <th className="px-3 py-2 font-normal">p50</th>
              <th className="px-3 py-2 font-normal">p95</th>
              <th className="px-3 py-2 font-normal">p99</th>
              <th className="px-3 py-2 font-normal">Max</th>
              <th className="px-3 py-2 font-normal">Errors</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.topology} className="border-b border-black/5 last:border-0 dark:border-white/5">
                <td className="px-3 py-2 font-mono">{r.topology}</td>
                <td className="px-3 py-2">{r.totalRequests.toLocaleString()}</td>
                <td className="px-3 py-2">{r.achievedRPS.toFixed(1)}</td>
                <td className="px-3 py-2">{r.p50Ms.toFixed(1)}ms</td>
                <td className="px-3 py-2">{r.p95Ms.toFixed(1)}ms</td>
                <td className="px-3 py-2">{r.p99Ms.toFixed(1)}ms</td>
                <td className="px-3 py-2">{r.maxMs.toFixed(1)}ms</td>
                <td
                  className={`px-3 py-2 ${
                    r.errorRate > 0.05
                      ? "font-medium text-red-600 dark:text-red-400"
                      : r.errorRate > 0
                        ? "text-amber-600 dark:text-amber-400"
                        : ""
                  }`}
                >
                  {(r.errorRate * 100).toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.some((r) => r.note) && (
          <div className="border-t border-black/10 px-3 py-2 text-xs text-black/50 dark:border-white/10 dark:text-white/50">
            {rows
              .filter((r) => r.note)
              .map((r) => (
                <div key={r.topology}>
                  {r.topology}: {r.note}
                </div>
              ))}
          </div>
        )}
      </div>
    </section>
  );
}
