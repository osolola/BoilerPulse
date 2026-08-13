"use client";

import Link from "next/link";
import { usePoll } from "@/lib/usePoll";
import { getClusterStatus, getWorkloadStatus, getEvents } from "@/lib/api";
import { StatusBadge } from "@/components/status-badge";
import { LevelBadge } from "@/components/level-badge";
import { PredictForm } from "@/components/predict-form";

export default function DashboardPage() {
  const cluster = usePoll(getClusterStatus, 3000);
  const workload = usePoll(getWorkloadStatus, 3000);
  const events = usePoll(getEvents, 5000);

  return (
    <main className="mx-auto max-w-5xl px-6 py-10">
      <h1 className="text-2xl font-semibold tracking-tight">Campus Dashboard</h1>
      <p className="mt-1 text-sm text-black/60 dark:text-white/60">
        Live status of the BoilerPulse cluster, via the gateway.
      </p>

      <div className="mt-8 grid gap-6 sm:grid-cols-2">
        <section className="rounded-lg border border-black/10 p-6 dark:border-white/10">
          <h2 className="text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
            System status
          </h2>
          {cluster.status === "loading" && <p className="mt-3 text-sm">Connecting to the gateway...</p>}
          {cluster.status === "error" && (
            <p className="mt-3 text-sm text-red-600 dark:text-red-400">
              Could not reach the gateway ({cluster.message}). Run <code>make cluster</code> in
              the repo root, or set <code>NEXT_PUBLIC_API_URL</code>.
            </p>
          )}
          {cluster.status === "ok" && (
            <div className="mt-3 space-y-2 text-sm">
              <div>
                Mode: <span className="font-mono">{cluster.data.mode}</span>
              </div>
              {cluster.data.nodes.map((node) => (
                <div key={node.id} className="flex items-center gap-3">
                  <span className="font-mono">{node.id}</span>
                  <span className="text-black/50 dark:text-white/50">{node.role}</span>
                  <StatusBadge label={node.reachable ? "HEALTHY" : "ERROR"} />
                </div>
              ))}
              <Link href="/cluster" className="mt-2 inline-block text-xs text-black/50 underline dark:text-white/50">
                view topology
              </Link>
            </div>
          )}
        </section>

        <section className="rounded-lg border border-black/10 p-6 dark:border-white/10">
          <h2 className="text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
            Workload
          </h2>
          {workload.status === "loading" && <p className="mt-3 text-sm">Loading...</p>}
          {workload.status === "error" && (
            <p className="mt-3 text-sm text-red-600 dark:text-red-400">{workload.message}</p>
          )}
          {workload.status === "ok" && (
            <div className="mt-3 space-y-3">
              <LevelBadge level={workload.data.mode} />
              <div className="text-sm">
                <span className="text-2xl font-semibold tabular-nums">{workload.data.rps.toFixed(1)}</span>
                <span className="ml-1 text-black/50 dark:text-white/50">req/s</span>
              </div>
              <Link href="/metrics" className="inline-block text-xs text-black/50 underline dark:text-white/50">
                view metrics
              </Link>
            </div>
          )}
        </section>
      </div>

      <section className="mt-6 rounded-lg border border-black/10 p-6 dark:border-white/10">
        <h2 className="text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
          Upcoming events
        </h2>
        {events.status === "loading" && <p className="mt-3 text-sm">Loading...</p>}
        {events.status === "error" && (
          <p className="mt-3 text-sm text-red-600 dark:text-red-400">{events.message}</p>
        )}
        {events.status === "ok" && events.data.events.length === 0 && (
          <p className="mt-3 text-sm text-black/60 dark:text-white/60">
            No events yet — start <code>cmd/ingest</code> (included in <code>make cluster</code>).
          </p>
        )}
        {events.status === "ok" && events.data.events.length > 0 && (
          <ul className="mt-3 space-y-2 text-sm">
            {events.data.events.slice(0, 5).map((e) => (
              <li key={e.id} className="flex items-center justify-between gap-3">
                <span>
                  {e.title} <span className="text-black/40 dark:text-white/40">— {e.location.name}</span>
                </span>
                <LevelBadge level={e.urgency} />
              </li>
            ))}
          </ul>
        )}
        <Link href="/events" className="mt-3 inline-block text-xs text-black/50 underline dark:text-white/50">
          view all events
        </Link>
      </section>

      <section className="mt-6 rounded-lg border border-black/10 p-6 dark:border-white/10">
        <h2 className="mb-4 text-sm font-medium uppercase tracking-wide text-black/50 dark:text-white/50">
          Predicted traffic
        </h2>
        <PredictForm />
      </section>
    </main>
  );
}
