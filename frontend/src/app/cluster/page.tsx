"use client";

import { usePoll } from "@/lib/usePoll";
import { getClusterStatus, type ClusterNode } from "@/lib/api";
import { StatusBadge } from "@/components/status-badge";
import { ChaosControls } from "./chaos-controls";

export default function ClusterPage() {
  const state = usePoll(getClusterStatus, 2000);

  return (
    <main className="mx-auto max-w-4xl px-6 py-10">
      <h1 className="text-2xl font-semibold tracking-tight">Distributed Cluster</h1>
      <p className="mt-1 text-sm text-black/60 dark:text-white/60">
        Live view from the gateway (<code>GET /v1/cluster</code>, refreshes every 2s) — real
        per-node reachability and Raft role, not simulated.
      </p>

      {state.status === "loading" && <p className="mt-8 text-sm">Connecting to the gateway...</p>}
      {state.status === "error" && (
        <p className="mt-8 text-sm text-red-600 dark:text-red-400">
          Could not reach the gateway ({state.message}). Run <code>make cluster</code> in the
          repo root, or set <code>NEXT_PUBLIC_API_URL</code>.
        </p>
      )}

      {state.status === "ok" && (
        <div className="mt-8">
          <div className="mb-6 text-sm">
            Mode: <span className="font-mono">{state.data.mode}</span>
            {state.data.term !== undefined && (
              <>
                {" "}
                &middot; Term: <span className="font-mono">{state.data.term}</span>
              </>
            )}
          </div>
          <Topology nodes={state.data.nodes} leaderID={state.data.leader_id} />
          <ChaosControls nodes={state.data.nodes} />
        </div>
      )}
    </main>
  );
}

function Topology({ nodes, leaderID }: { nodes: ClusterNode[]; leaderID?: string }) {
  const leader = nodes.find((n) => n.id === leaderID || n.role === "LEADER");
  const followers = nodes.filter((n) => n !== leader);

  return (
    <div className="flex flex-col items-center gap-8">
      {leader && <NodeCard node={leader} isLeader />}
      {followers.length > 0 && (
        <>
          <div className="h-6 w-px bg-black/15 dark:bg-white/15" />
          <div className="flex flex-wrap justify-center gap-4">
            {followers.map((n) => (
              <NodeCard key={n.id} node={n} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function NodeCard({ node, isLeader }: { node: ClusterNode; isLeader?: boolean }) {
  const reachable = node.reachable ?? node.status === "HEALTHY";
  return (
    <div
      className={`w-56 rounded-lg border p-4 ${
        isLeader
          ? "border-black/30 bg-black/[.03] dark:border-white/30 dark:bg-white/[.05]"
          : "border-black/10 dark:border-white/10"
      }`}
    >
      <div className="font-mono text-sm">{node.id}</div>
      <div className="mt-1 flex items-center justify-between text-xs">
        <span className="text-black/50 dark:text-white/50">{node.role || "UNKNOWN"}</span>
        <StatusBadge label={reachable ? "HEALTHY" : "ERROR"} />
      </div>
      {node.addr && <div className="mt-2 truncate text-xs text-black/40 dark:text-white/40">{node.addr}</div>}
      {node.term !== undefined && (
        <div className="mt-1 text-xs text-black/40 dark:text-white/40">term {node.term}</div>
      )}
    </div>
  );
}
