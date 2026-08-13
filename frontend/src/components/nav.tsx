import Link from "next/link";

const LINKS = [
  { href: "/", label: "Dashboard" },
  { href: "/events", label: "Events" },
  { href: "/cluster", label: "Cluster" },
  { href: "/simulation", label: "Simulation" },
  { href: "/metrics", label: "Metrics" },
  { href: "/about", label: "About" },
];

export function Nav() {
  return (
    <nav className="border-b border-black/10 dark:border-white/10">
      <div className="mx-auto flex max-w-5xl items-center gap-6 px-6 py-4">
        <span className="font-semibold tracking-tight">BoilerPulse</span>
        <div className="flex gap-4 text-sm text-black/70 dark:text-white/70">
          {LINKS.map((link) => (
            <Link key={link.href} href={link.href} className="hover:text-black dark:hover:text-white">
              {link.label}
            </Link>
          ))}
        </div>
      </div>
    </nav>
  );
}
