"use client";

import { usePathname } from "next/navigation";
import { Menu, RefreshCw, User } from "lucide-react";

import { useClusterHealth } from "@/components/health/HealthProvider";
import { requestPageRefresh } from "@/lib/events";

const titles: Record<string, string> = {
  "/": "Cluster Overview",
  "/files": "File Manager",
  "/settings": "Settings",
};

type TopbarProps = {
  onMenu: () => void;
};

export default function Topbar({ onMenu }: TopbarProps) {
  const pathname = usePathname();
  const { state, refresh } = useClusterHealth();
  const checking = state === "checking";
  const title = titles[pathname] ?? "Console";

  const handleRefresh = () => {
    refresh();
    requestPageRefresh();
  };

  return (
    <header className="sticky top-0 z-30 flex h-16 shrink-0 items-center justify-between gap-4 border-b border-zinc-800 bg-zinc-950/90 px-4 backdrop-blur sm:px-6 lg:px-8">
      <div className="flex min-w-0 items-center gap-3">
        <button
          type="button"
          onClick={onMenu}
          aria-label="Open navigation"
          className="grid h-9 w-9 shrink-0 place-items-center rounded-lg text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-50 lg:hidden"
        >
          <Menu className="h-4 w-4" />
        </button>
        <h1 className="truncate text-sm font-semibold text-zinc-50">
          {title}
        </h1>
      </div>

      <div className="flex items-center gap-2 sm:gap-3">
        <button
          type="button"
          onClick={handleRefresh}
          disabled={checking}
          className="inline-flex h-9 items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-900 px-3 text-xs font-medium text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-50 disabled:cursor-wait disabled:opacity-60"
        >
          <RefreshCw
            className={`h-3.5 w-3.5 ${checking ? "animate-spin" : ""}`}
          />
          <span className="hidden sm:inline">Refresh</span>
        </button>

        <div className="flex items-center gap-2">
          <div className="hidden text-right sm:block">
            <p className="text-xs font-medium text-zinc-200">admin</p>
            <p className="text-[10px] text-zinc-500">Administrator</p>
          </div>
          <div className="grid h-8 w-8 place-items-center rounded-full border border-zinc-700 bg-zinc-800 text-zinc-400">
            <User className="h-4 w-4" />
          </div>
        </div>
      </div>
    </header>
  );
}
