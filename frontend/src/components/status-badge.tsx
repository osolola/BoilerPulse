const COLORS: Record<string, string> = {
  HEALTHY: "bg-green-500",
  ok: "bg-green-500",
  ERROR: "bg-red-500",
};

export function StatusBadge({ label }: { label: string }) {
  const color = COLORS[label] ?? "bg-yellow-500";
  return (
    <span className="inline-flex items-center gap-2 text-sm">
      <span className={`h-2.5 w-2.5 rounded-full ${color}`} />
      {label}
    </span>
  );
}
