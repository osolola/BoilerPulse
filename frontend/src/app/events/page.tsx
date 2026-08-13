"use client";

import { usePoll } from "@/lib/usePoll";
import { getEvents, type CampusEvent } from "@/lib/api";
import { LevelBadge } from "@/components/level-badge";

// Cap how many rows render: with cmd/ingest running continuously, the
// event count grows unboundedly over time (there's no expiry/archival —
// see docs/event-ingestion.md), and rendering every single one as one long
// page is bad UX at scale. This was found by actually loading the page
// after a long-running ingest session during Milestone 8 testing, not by
// reasoning about it in the abstract.
const MAX_DISPLAYED = 50;

export default function EventsPage() {
  const state = usePoll(getEvents, 5000);

  return (
    <main className="mx-auto max-w-4xl px-6 py-10">
      <h1 className="text-2xl font-semibold tracking-tight">Event Feed</h1>
      <p className="mt-1 text-sm text-black/60 dark:text-white/60">
        Normalized campus events, ingested and replicated through the Raft cluster
        (<code>GET /v1/events</code>, refreshes every 5s).
      </p>

      <div className="mt-8">
        {state.status === "loading" && <p className="text-sm">Loading events...</p>}
        {state.status === "error" && (
          <p className="text-sm text-red-600 dark:text-red-400">
            Could not reach the gateway ({state.message}). Run <code>make cluster</code> in
            the repo root, or set <code>NEXT_PUBLIC_API_URL</code>.
          </p>
        )}
        {state.status === "ok" && state.data.events.length === 0 && (
          <p className="text-sm text-black/60 dark:text-white/60">
            No events yet — start <code>cmd/ingest</code> (included in <code>make cluster</code>)
            to generate synthetic campus events.
          </p>
        )}
        {state.status === "ok" && state.data.events.length > 0 && (
          <>
            <EventList events={state.data.events} />
          </>
        )}
      </div>
    </main>
  );
}

function EventList({ events }: { events: CampusEvent[] }) {
  const shown = events.slice(0, MAX_DISPLAYED);
  return (
    <>
      {events.length > MAX_DISPLAYED && (
        <p className="mb-3 text-xs text-black/50 dark:text-white/50">
          Showing the soonest {MAX_DISPLAYED} of {events.length} events.
        </p>
      )}
      <ul className="space-y-3">
        {shown.map((e) => (
          <EventRow key={e.id} event={e} />
        ))}
      </ul>
    </>
  );
}

function EventRow({ event }: { event: CampusEvent }) {
  return (
    <li className="rounded-lg border border-black/10 p-4 dark:border-white/10">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <span className="font-medium">{event.title}</span>
            <LevelBadge level={event.urgency} />
          </div>
          <div className="mt-1 text-xs text-black/50 dark:text-white/50">
            {event.type} &middot; {event.location.name || "Campus-wide"}
            {event.expected_attendance ? ` · ~${event.expected_attendance.toLocaleString()} expected` : ""}
          </div>
        </div>
        <div className="whitespace-nowrap text-right text-xs text-black/50 dark:text-white/50">
          {new Date(event.start_time).toLocaleString()}
        </div>
      </div>
    </li>
  );
}
