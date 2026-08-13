"use client";

import { useEffect, useState } from "react";

export type PollState<T> =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ok"; data: T };

// Generic polling hook: calls fetcher immediately and then every pollMs,
// tracking loading/error/ok state. Used by every dashboard page that shows
// live backend data (cluster, workload, events).
export function usePoll<T>(fetcher: () => Promise<T>, pollMs = 5000): PollState<T> {
  const [state, setState] = useState<PollState<T>>({ status: "loading" });

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const data = await fetcher();
        if (!cancelled) setState({ status: "ok", data });
      } catch (err) {
        if (!cancelled) {
          setState({
            status: "error",
            message: err instanceof Error ? err.message : "unknown error",
          });
        }
      }
    }

    poll();
    const id = setInterval(poll, pollMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pollMs]);

  return state;
}
