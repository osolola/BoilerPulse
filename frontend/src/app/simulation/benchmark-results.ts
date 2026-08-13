// Real recorded benchmark runs (Milestone 10) -- copied verbatim from the
// JSON reports in benchmarks/results/, produced by actually running
// cmd/simulator against a compiled single-node and 3-node cluster on
// 2026-08-12. Not live, not estimated: a snapshot of one real run each.
// See docs/benchmarking.md for full methodology and analysis.
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

export const RECORDED_AT = "2026-08-12";

export const BENCHMARK_RESULTS: BenchmarkResult[] = [
  { scenario: "normal", topology: "single-node", totalRequests: 199, achievedRPS: 9.9, p50Ms: 1.6, p95Ms: 6.8, p99Ms: 8.2, maxMs: 8.3, errorRate: 0 },
  { scenario: "normal", topology: "3-node", totalRequests: 199, achievedRPS: 9.9, p50Ms: 2.0, p95Ms: 18.1, p99Ms: 19.2, maxMs: 19.8, errorRate: 0 },
  { scenario: "finals", topology: "single-node", totalRequests: 1549, achievedRPS: 51.6, p50Ms: 1.1, p95Ms: 6.1, p99Ms: 6.8, maxMs: 40.6, errorRate: 0 },
  { scenario: "finals", topology: "3-node", totalRequests: 1548, achievedRPS: 51.6, p50Ms: 1.4, p95Ms: 17.4, p99Ms: 27.1, maxMs: 445.8, errorRate: 0 },
  { scenario: "athletics", topology: "single-node", totalRequests: 4193, achievedRPS: 119.8, p50Ms: 1.3, p95Ms: 6.5, p99Ms: 10.0, maxMs: 36.1, errorRate: 0 },
  { scenario: "athletics", topology: "3-node", totalRequests: 4197, achievedRPS: 119.9, p50Ms: 1.3, p95Ms: 17.1, p99Ms: 36.0, maxMs: 774.6, errorRate: 0.0002 },
  { scenario: "emergency", topology: "single-node", totalRequests: 3629, achievedRPS: 172.8, p50Ms: 4.6, p95Ms: 11.5, p99Ms: 14.6, maxMs: 21.4, errorRate: 0 },
  { scenario: "emergency", topology: "3-node", totalRequests: 3593, achievedRPS: 145.2, p50Ms: 1.5, p95Ms: 14.6, p99Ms: 540.9, maxMs: 1554.4, errorRate: 0.3813, note: "real write-throughput ceiling -- see docs/benchmarking.md" },
  { scenario: "hotkey", topology: "single-node", totalRequests: 599, achievedRPS: 29.9, p50Ms: 1.3, p95Ms: 5.5, p99Ms: 7.3, maxMs: 10.1, errorRate: 0 },
  { scenario: "hotkey", topology: "3-node", totalRequests: 413, achievedRPS: 20.6, p50Ms: 1.1, p95Ms: 16.0, p99Ms: 18.5, maxMs: 28.8, errorRate: 0 },
  { scenario: "athletics", topology: "3-node-failure", totalRequests: 4118, achievedRPS: 117.6, p50Ms: 1.2, p95Ms: 19.2, p99Ms: 483.6, maxMs: 848.9, errorRate: 0.0019, note: "leader killed at t=15s -- see docs/benchmarking.md" },
];
