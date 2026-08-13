// Renders urgency (NORMAL/HIGH/CRITICAL/SCHEDULED_SPIKE) and workload mode
// (NORMAL/ELEVATED/HIGH_TRAFFIC/CRITICAL) values with consistent coloring —
// the two enums share NORMAL and CRITICAL, so one component covers both.
const COLORS: Record<string, string> = {
  NORMAL: "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300",
  ELEVATED: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300",
  HIGH: "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300",
  HIGH_TRAFFIC: "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300",
  SCHEDULED_SPIKE: "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300",
  CRITICAL: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300",
};

export function LevelBadge({ level }: { level: string }) {
  const classes = COLORS[level] ?? "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300";
  return (
    <span className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${classes}`}>
      {level}
    </span>
  );
}
