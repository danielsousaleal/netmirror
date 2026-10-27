"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  FolderOpen,
  Layers3,
  LayoutDashboard,
  Settings,
  X,
} from "lucide-react";

import { useClusterHealth } from "@/components/health/HealthProvider";

const navigation = [
  { href: "/", label: "Overview", icon: LayoutDashboard },
  { href: "/files", label: "File Manager", icon: FolderOpen },
  { href: "/settings", label: "Settings", icon: Settings },
] as const;

type SidebarProps = {
  open: boolean;
  onClose: () => void;
};

export default function Sidebar({ open, onClose }: SidebarProps) {
  const pathname = usePathname();
  const { state, stats } = useClusterHealth();

  const online = state === "online";
  const checking = state === "checking";
  const statusLabel = checking
    ? "Checking Cluster"
    : online
      ? "Cluster Operational"
      : "Cluster Warning";
  const statusDot =
    online && !checking
      ? "bg-emerald-500 animate-pulse"
      : "bg-amber-500 animate-pulse";
  const statusText =
    online && !checking ? "text-emerald-500" : "text-amber-500";

  return (
    <>
      {open ? (
        <button
          type="button"
          aria-label="Close navigation"
          onClick={onClose}
          className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm lg:hidden"
        />
      ) : null}

      <aside
        className={`fixed inset-y-0 left-0 z-50 flex h-screen w-64 shrink-0 -translate-x-full flex-col border-r border-zinc-800 bg-zinc-900 px-4 py-5 transition-transform lg:sticky lg:top-0 lg:z-auto lg:translate-x-0 ${open ? "translate-x-0" : ""}`}
      >
        <div className="flex items-center justify-between px-2">
          <Link href="/" onClick={onClose} className="flex items-center gap-2.5">
            <span className="grid h-9 w-9 place-items-center rounded-lg border border-emerald-500/20 bg-emerald-500/10 text-emerald-500">
              <Layers3 className="h-4 w-4" />
            </span>
            <span>
              <span className="block text-sm font-semibold tracking-tight text-zinc-50">
                Netmirror
              </span>
              <span className="block text-[11px] text-zinc-500">
                Storage Console
              </span>
            </span>
          </Link>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close navigation"
            className="grid h-8 w-8 place-items-center rounded-lg text-zinc-500 transition-colors hover:bg-zinc-800 hover:text-zinc-200 lg:hidden"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <nav aria-label="Primary" className="mt-8 space-y-1">
          {navigation.map((item) => {
            const active =
              item.href === "/"
                ? pathname === "/"
                : pathname.startsWith(item.href);
            const Icon = item.icon;

            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={onClose}
                aria-current={active ? "page" : undefined}
                className={`flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                  active
                    ? "bg-zinc-800 text-zinc-50"
                    : "text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200"
                }`}
              >
                <Icon className="h-4 w-4" />
                {item.label}
              </Link>
            );
          })}
        </nav>

        <div className="mt-auto border-t border-zinc-800 pt-4">
          <div className="flex items-center gap-2 px-3 py-2">
            <span className={`h-2 w-2 rounded-full ${statusDot}`} />
            <span className={`text-xs font-medium ${statusText}`}>
              {statusLabel}
            </span>
          </div>
          <p className="mt-1 px-3 text-[11px] text-zinc-600">
            {stats?.nodes.length ?? "—"} nodes · v2.0
          </p>
        </div>
      </aside>
    </>
  );
}
