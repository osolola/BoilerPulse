"use client";

import { useEffect, useState } from "react";
import {
  adminFault,
  adminKill,
  adminRestore,
  getAdminStatusAll,
  type AdminStatusAll,
  type ClusterNode,
} from "@/lib/api";

// ChaosControls drives real failure injection against the running cluster
// (spec §23/§34): kill, partition, latency, packet-drop, restore -- all
// through the gateway's token-gated /v1/admin/* proxy
// (internal/gateway/admin_proxy.go), which forwards to each node's own
// admin server (internal/admin). The token lives only in component state
// (not persisted) since this is deliberately destructive, opt-in tooling,
// not something to leave lying around in localStorage.
export function ChaosControls({ nodes }: { nodes: ClusterNode[] }) {
  const [token, setToken] = useState("");
  const [unlocked, setUnlocked] = useState(false);
  const [status, setStatus] = useState<AdminStatusAll | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null); // "node-1:kill" etc, disables that action

  useEffect(() => {
    if (!unlocked) return;
    let cancelled = false;

    async function poll() {
      try {
        const data = await getAdminStatusAll(token);
        if (!cancelled) {
          setStatus(data);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "unknown error");
        }
      }
    }

    poll();
    const id = setInterval(poll, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [unlocked, token]);

  if (!unlocked) {
    return (
      <div className="mt-10 rounded-lg border border-black/10 p-4 dark:border-white/10">
        <div className="text-sm font-medium">Chaos controls</div>
        <p className="mt-1 text-xs text-black/60 dark:text-white/60">
          Kill, partition, add latency to, or drop packets from a real node. Gated by the
          cluster&apos;s admin token (<code>BOILERPULSE_ADMIN_TOKEN</code>; <code>make cluster</code>{" "}
          writes one to <code>.cluster/admin.token</code> if you didn&apos;t set your own).
        </p>
        <form
          className="mt-3 flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (token.trim()) setUnlocked(true);
          }}
        >
          <input
            type="password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="admin token"
            className="w-64 rounded border border-black/15 bg-transparent px-2 py-1 text-sm dark:border-white/20"
          />
          <button
            type="submit"
            className="rounded border border-black/15 px-3 py-1 text-sm hover:bg-black/5 dark:border-white/20 dark:hover:bg-white/10"
          >
            Unlock
          </button>
        </form>
      </div>
    );
  }

  return (
    <div className="mt-10">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium">Chaos controls</div>
        <button
          className="text-xs text-black/50 underline dark:text-white/50"
          onClick={() => {
            setUnlocked(false);
            setStatus(null);
            setToken("");
          }}
        >
          lock
        </button>
      </div>
      {error && (
        <p className="mt-2 text-xs text-red-600 dark:text-red-400">
          {error} — wrong token, or the gateway has no BOILERPULSE_ADMIN_TOKEN configured.
        </p>
      )}
      <div className="mt-3 grid gap-3 sm:grid-cols-3">
        {nodes.map((n) => (
          <NodeChaosCard
            key={n.id}
            nodeID={n.id}
            token={token}
            status={status?.[n.id]}
            busy={busy}
            setBusy={setBusy}
          />
        ))}
      </div>
    </div>
  );
}

function isErrorStatus(s: unknown): s is { error: string } {
  return !!s && typeof s === "object" && "error" in s;
}

function NodeChaosCard({
  nodeID,
  token,
  status,
  busy,
  setBusy,
}: {
  nodeID: string;
  token: string;
  status: AdminStatusAll[string] | undefined;
  busy: string | null;
  setBusy: (key: string | null) => void;
}) {
  const [latencyMs, setLatencyMs] = useState("200");
  const [dropRate, setDropRate] = useState("0.5");
  const [actionError, setActionError] = useState<string | null>(null);

  const key = (action: string) => `${nodeID}:${action}`;
  const isBusy = (action: string) => busy === key(action);

  async function run(action: string, fn: () => Promise<unknown>) {
    setBusy(key(action));
    setActionError(null);
    try {
      await fn();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "unknown error");
    } finally {
      setBusy(null);
    }
  }

  const unreachable = isErrorStatus(status);
  const partitioned = !unreachable && status?.faults.partitioned;
  const latencyActive = !unreachable && (status?.faults.latency_ms ?? 0) > 0;
  const dropActive = !unreachable && (status?.faults.drop_rate ?? 0) > 0;

  return (
    <div className="rounded-lg border border-black/10 p-3 text-xs dark:border-white/10">
      <div className="flex items-center justify-between">
        <div className="font-mono text-sm">{nodeID}</div>
        {unreachable ? (
          <span className="text-red-600 dark:text-red-400">unreachable</span>
        ) : (
          <span className="text-black/50 dark:text-white/50">{status?.raft.state ?? "..."}</span>
        )}
      </div>

      {!unreachable && (
        <div className="mt-1 flex flex-wrap gap-2 text-[10px] text-black/50 dark:text-white/50">
          {partitioned && <span className="text-red-600 dark:text-red-400">partitioned</span>}
          {latencyActive && <span>+{status?.faults.latency_ms}ms</span>}
          {dropActive && <span>drop {status?.faults.drop_rate}</span>}
          {!partitioned && !latencyActive && !dropActive && <span>no faults</span>}
        </div>
      )}

      <div className="mt-3 flex flex-col gap-2">
        <button
          disabled={isBusy("kill")}
          onClick={() => {
            if (!confirm(`Kill ${nodeID}? This ungracefully exits the real process.`)) return;
            run("kill", () => adminKill(nodeID, token));
          }}
          className="rounded border border-red-600/30 px-2 py-1 text-red-600 hover:bg-red-600/10 disabled:opacity-50 dark:text-red-400"
        >
          {isBusy("kill") ? "killing…" : "Kill"}
        </button>

        <button
          disabled={isBusy("partition")}
          onClick={() => run("partition", () => adminFault(nodeID, token, { partitioned: !partitioned }))}
          className="rounded border border-black/15 px-2 py-1 hover:bg-black/5 disabled:opacity-50 dark:border-white/20 dark:hover:bg-white/10"
        >
          {isBusy("partition") ? "…" : partitioned ? "Heal partition" : "Partition"}
        </button>

        <div className="flex items-center gap-1">
          <input
            type="number"
            min={0}
            value={latencyMs}
            onChange={(e) => setLatencyMs(e.target.value)}
            className="w-16 rounded border border-black/15 bg-transparent px-1 py-1 dark:border-white/20"
          />
          <span className="text-black/50 dark:text-white/50">ms</span>
          <button
            disabled={isBusy("latency")}
            onClick={() => run("latency", () => adminFault(nodeID, token, { latency_ms: Number(latencyMs) || 0 }))}
            className="ml-auto rounded border border-black/15 px-2 py-1 hover:bg-black/5 disabled:opacity-50 dark:border-white/20 dark:hover:bg-white/10"
          >
            Apply
          </button>
        </div>

        <div className="flex items-center gap-1">
          <input
            type="number"
            min={0}
            max={1}
            step={0.1}
            value={dropRate}
            onChange={(e) => setDropRate(e.target.value)}
            className="w-16 rounded border border-black/15 bg-transparent px-1 py-1 dark:border-white/20"
          />
          <span className="text-black/50 dark:text-white/50">drop</span>
          <button
            disabled={isBusy("drop")}
            onClick={() => run("drop", () => adminFault(nodeID, token, { drop_rate: Number(dropRate) || 0 }))}
            className="ml-auto rounded border border-black/15 px-2 py-1 hover:bg-black/5 disabled:opacity-50 dark:border-white/20 dark:hover:bg-white/10"
          >
            Apply
          </button>
        </div>

        <button
          disabled={isBusy("restore")}
          onClick={() => run("restore", () => adminRestore(nodeID, token))}
          className="rounded border border-black/15 px-2 py-1 hover:bg-black/5 disabled:opacity-50 dark:border-white/20 dark:hover:bg-white/10"
        >
          {isBusy("restore") ? "…" : "Restore"}
        </button>
      </div>

      {actionError && <p className="mt-2 text-red-600 dark:text-red-400">{actionError}</p>}
    </div>
  );
}
