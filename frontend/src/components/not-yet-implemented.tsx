export function NotYetImplemented({
  title,
  milestone,
  detail,
}: {
  title: string;
  milestone: string;
  detail: string;
}) {
  return (
    <div className="rounded-lg border border-dashed border-black/15 p-8 text-center dark:border-white/15">
      <h2 className="text-lg font-medium">{title}</h2>
      <p className="mt-2 text-sm text-black/60 dark:text-white/60">{detail}</p>
      <p className="mt-4 text-xs uppercase tracking-wide text-black/40 dark:text-white/40">
        Arrives in {milestone}
      </p>
    </div>
  );
}
