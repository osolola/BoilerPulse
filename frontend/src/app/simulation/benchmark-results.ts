// Real recorded benchmark runs (Milestone 10, updated post-Milestone-11
// Day 3's group-commit fix) -- copied verbatim from the JSON reports in
// benchmarks/results/, produced by actually running cmd/simulator against
// a compiled single-node and 3-node cluster on 2026-09-14. Not live, not
// estimated: a snapshot of one real run each. See docs/benchmarking.md for
// full methodology and analysis, including why the emergency/3-node row
// below didn't improve even though every other write-heavy row did.
export type BenchmarkResult = {
  scenario: string;
  topology: string;
  totalRequests: number;
  achievedRPS: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
  errorRate: number;
  note?: string;
};

export const RECORDED_AT = "2026-09-14";

export const BENCHMARK_RESULTS: BenchmarkResult[] = [
  { scenario: "normal", topology: "single-node", totalRequests: 199, achievedRPS: 9.9, p50Ms: 0.7, p95Ms: 6.2, p99Ms: 8.0, maxMs: 9.0, errorRate: 0 },
  { scenario: "normal", topology: "3-node", totalRequests: 199, achievedRPS: 9.9, p50Ms: 1.5, p95Ms: 17.1, p99Ms: 20.1, maxMs: 112.6, errorRate: 0 },
  { scenario: "finals", topology: "single-node", totalRequests: 1547, achievedRPS: 51.6, p50Ms: 0.7, p95Ms: 6.0, p99Ms: 13.0, maxMs: 142.6, errorRate: 0 },
  { scenario: "finals", topology: "3-node", totalRequests: 1549, achievedRPS: 51.6, p50Ms: 1.2, p95Ms: 16.5, p99Ms: 20.5, maxMs: 35.2, errorRate: 0 },
  { scenario: "athletics", topology: "single-node", totalRequests: 4167, achievedRPS: 119.1, p50Ms: 0.9, p95Ms: 5.9, p99Ms: 13.7, maxMs: 286.6, errorRate: 0 },
  { scenario: "athletics", topology: "3-node", totalRequests: 4189, achievedRPS: 119.7, p50Ms: 0.8, p95Ms: 16.2, p99Ms: 26.8, maxMs: 50.0, errorRate: 0 },
  { scenario: "emergency", topology: "single-node", totalRequests: 3609, achievedRPS: 171.8, p50Ms: 1.1, p95Ms: 9.6, p99Ms: 14.5, maxMs: 135.4, errorRate: 0 },
  { scenario: "emergency", topology: "3-node", totalRequests: 3629, achievedRPS: 117.8, p50Ms: 1.2, p95Ms: 290.4, p99Ms: 1423.0, maxMs: 1955.2, errorRate: 0.3621, note: "the propose-path fix worked (commit rate is healthy), which exposed a second bottleneck in the sequential apply path -- see docs/benchmarking.md" },
  { scenario: "hotkey", topology: "single-node", totalRequests: 598, achievedRPS: 29.9, p50Ms: 1.1, p95Ms: 5.1, p99Ms: 6.9, maxMs: 9.7, errorRate: 0 },
  { scenario: "hotkey", topology: "3-node", totalRequests: 599, achievedRPS: 29.9, p50Ms: 1.0, p95Ms: 15.6, p99Ms: 18.6, maxMs: 26.4, errorRate: 0 },
  { scenario: "athletics", topology: "3-node-failure", totalRequests: 4163, achievedRPS: 118.9, p50Ms: 1.0, p95Ms: 16.4, p99Ms: 29.5, maxMs: 145.1, errorRate: 0.0022, note: "leader killed at t=15s -- group-commit batching cut failover p99 from 484ms to 29.5ms, see docs/benchmarking.md" },
];
