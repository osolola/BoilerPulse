"use client";

import { useState } from "react";
import { predictTraffic, type PredictionOutput } from "@/lib/api";

const EVENT_TYPES = [
  "ATHLETICS",
  "ACADEMIC",
  "STUDENT_ORG",
  "CAMPUS_EVENT",
  "DINING",
  "TRANSPORTATION",
  "WEATHER",
  "EMERGENCY",
  "SYSTEM",
];

// Interactive form for POST /v1/predict (spec §31's "PREDICTED PEAK" dashboard
// element, backed by the real model from internal/prediction — Milestone 7).
export function PredictForm() {
  const [type, setType] = useState("ATHLETICS");
  const [attendance, setAttendance] = useState(5000);
  const [startTime, setStartTime] = useState(() => {
    const d = new Date(Date.now() + 24 * 60 * 60 * 1000);
    return d.toISOString().slice(0, 16);
  });
  const [result, setResult] = useState<PredictionOutput | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const output = await predictTraffic({
        type,
        title: "Prediction preview",
        start_time: new Date(startTime).toISOString(),
        expected_attendance: attendance,
      });
      setResult(output);
    } catch (err) {
      setError(err instanceof Error ? err.message : "prediction failed");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <label className="text-sm">
          <span className="mb-1 block text-black/60 dark:text-white/60">Event type</span>
          <select
            value={type}
            onChange={(e) => setType(e.target.value)}
            className="w-full rounded border border-black/15 bg-transparent px-2 py-1.5 dark:border-white/15"
          >
            {EVENT_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-black/60 dark:text-white/60">Expected attendance</span>
          <input
            type="number"
            min={0}
            value={attendance}
            onChange={(e) => setAttendance(Number(e.target.value))}
            className="w-full rounded border border-black/15 bg-transparent px-2 py-1.5 dark:border-white/15"
          />
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-black/60 dark:text-white/60">Start time</span>
          <input
            type="datetime-local"
            value={startTime}
            onChange={(e) => setStartTime(e.target.value)}
            className="w-full rounded border border-black/15 bg-transparent px-2 py-1.5 dark:border-white/15"
          />
        </label>
      </div>

      <button
        type="submit"
        disabled={loading}
        className="rounded bg-black px-4 py-1.5 text-sm text-white disabled:opacity-50 dark:bg-white dark:text-black"
      >
        {loading ? "Predicting..." : "Predict traffic"}
      </button>

      {error && <p className="text-sm text-red-600 dark:text-red-400">{error}</p>}

      {result && (
        <div className="grid grid-cols-2 gap-4 rounded-lg border border-black/10 p-4 sm:grid-cols-4 dark:border-white/10">
          <Stat label="Predicted RPS" value={result.predicted_rps.toFixed(1)} />
          <Stat label="Confidence" value={`${(result.confidence * 100).toFixed(0)}%`} />
          <Stat label="Recommended nodes" value={String(result.recommended_nodes)} />
          <Stat label="Peak time" value={new Date(result.peak_time).toLocaleString()} />
        </div>
      )}
    </form>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-lg font-semibold tabular-nums">{value}</div>
      <div className="text-xs text-black/50 dark:text-white/50">{label}</div>
    </div>
  );
}
