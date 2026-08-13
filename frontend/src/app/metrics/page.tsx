"use client";

import { useEffect, useRef, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { usePoll } from "@/lib/usePoll";
import { getWorkloadStatus, type WorkloadStatus } from "@/lib/api";
import { LevelBadge } from "@/components/level-badge";

type RPSPoint = { time: string; rps: number };

const MAX_POINTS = 30;
const LINE_COLOR = "#3b82f6"; // Tailwind blue-500 -- single series, sequential/magnitude use
const BAR_COLOR = "#3b82f6";

export default function MetricsPage() {
  const state = usePoll(getWorkloadStatus, 2000);
  const [history, setHistory] = useState<RPSPoint[]>([]);
  const lastValue = useRef<number | null>(null);

  useEffect(() => {
    if (state.status !== "ok") return;
    const rps = state.data.rps;
    if (rps === lastValue.current) return; // avoid padding the chart with duplicate polls
    lastValue.current = rps;
    setHistory((prev) => {
      const next = [...prev, { time: new Date().toLocaleTimeString(), rps }];
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
    });
  }, [state]);

  return (
    <main className="mx-auto max-w-4xl px-6 py-10">
      <h1 className="text-2xl font-semibold tracking-tight">Performance Metrics</h1>
      <p className="mt-1 text-sm text-black/60 dark:text-white/60">
        Live from the gateway (<code>GET /v1/workload</code>, refreshes every 2s) — request
        rate, hot keys, and cache effectiveness computed from real proxied traffic.
      </p>

      {state.status === "loading" && <p className="mt-8 text-sm">Connecting to the gateway...</p>}
      {state.status === "error" && (
        <p className="mt-8 text-sm text-red-600 dark:text-red-400">
          Could not reach the gateway ({state.message}). Run <code>make cluster</code> in the
          repo root, or set <code>NEXT_PUBLIC_API_URL</code>.
        </p>
      )}

      {state.status === "ok" && (
        <div className="mt-8 space-y-10">
          <section>
            <div className="mb-3 flex items-center gap-3">
              <h2 className="text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
                Workload mode
              </h2>
              <LevelBadge level={state.data.mode} />
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
              Requests per second
            </h2>
            {history.length < 2 ? (
              <p className="text-sm text-black/50 dark:text-white/50">
                Collecting samples... ({history.length}/2)
              </p>
            ) : (
              <div className="h-56 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={history} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-black/10 dark:stroke-white/10" />
                    <XAxis dataKey="time" tick={{ fontSize: 11 }} minTickGap={30} />
                    <YAxis tick={{ fontSize: 11 }} width={40} />
                    <Tooltip
                      contentStyle={{ fontSize: 12, borderRadius: 8 }}
                      formatter={(value) => [Number(value).toFixed(2), "RPS"]}
                    />
                    <Line
                      type="monotone"
                      dataKey="rps"
                      stroke={LINE_COLOR}
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
              Hot keys
            </h2>
            {!state.data.hot_keys || state.data.hot_keys.length === 0 ? (
              <p className="text-sm text-black/50 dark:text-white/50">
                No keys currently over the hot-key threshold.
              </p>
            ) : (
              <div className="h-48 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={state.data.hot_keys} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-black/10 dark:stroke-white/10" />
                    <XAxis dataKey="key" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 11 }} width={30} allowDecimals={false} />
                    <Tooltip contentStyle={{ fontSize: 12, borderRadius: 8 }} />
                    <Bar dataKey="count" fill={BAR_COLOR} radius={[4, 4, 0, 0]} isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
              Cache
            </h2>
            <CacheStats stats={state.data.cache} />
          </section>
        </div>
      )}
    </main>
  );
}

function CacheStats({ stats }: { stats: WorkloadStatus["cache"] }) {
  const tiles = [
    { label: "Hit rate", value: `${(stats.hit_rate * 100).toFixed(1)}%` },
    { label: "Hits", value: stats.hits.toLocaleString() },
    { label: "Misses", value: stats.misses.toLocaleString() },
    { label: "Evictions", value: stats.evictions.toLocaleString() },
  ];
  return (
    <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
      {tiles.map((t) => (
        <div key={t.label} className="rounded-lg border border-black/10 p-4 dark:border-white/10">
          <div className="text-2xl font-semibold tabular-nums">{t.value}</div>
          <div className="mt-1 text-xs text-black/50 dark:text-white/50">{t.label}</div>
        </div>
      ))}
    </div>
  );
}
