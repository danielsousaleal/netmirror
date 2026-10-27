"use client";

import { useEffect, useState } from "react";

type HealthPayload = {
  status?: string;
  nodes?: number;
};

type ConnectionState = "checking" | "online" | "offline";

const features = [
  {
    title: "2-of-3 quorum",
    body: "Every write is acknowledged by at least two replicas before the gateway reports success.",
  },
  {
    title: "Content-addressed",
    body: "Objects are split into 4 MiB chunks named by SHA-256. Identical chunks are stored once.",
  },
  {
    title: "Self-healing",
    body: "Heartbeats detect failures and anti-entropy pulls missing manifests and chunks after downtime.",
  },
];

export default function Home() {
  const [state, setState] = useState<ConnectionState>("checking");
  const [nodes, setNodes] = useState<number | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    let cancelled = false;

    const check = async () => {
      try {
        const response = await fetch("/api/health", { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = (await response.json()) as HealthPayload;
        if (cancelled) {
          return;
        }
        setState(payload.status === "ok" ? "online" : "offline");
        setNodes(typeof payload.nodes === "number" ? payload.nodes : null);
      } catch {
        if (!cancelled) {
          setState("offline");
          setNodes(null);
        }
      } finally {
        if (!cancelled) {
          setLastChecked(new Date());
        }
      }
    };

    void check();
    const timer = window.setInterval(() => void check(), 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [refreshKey]);

  const online = state === "online";
  const checking = state === "checking";
  const statusLabel = checking
    ? "Checking cluster…"
    : online
      ? "Cluster Online"
      : "Cluster Offline";
  const statusDetail = checking
    ? "Contacting /api/health…"
    : online
      ? `${nodes ?? "?"} nodes reporting healthy`
      : "The gateway did not answer /api/health.";

  return (
    <main className="relative min-h-screen overflow-hidden">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_20%_0%,rgba(16,185,129,0.16),transparent_35%),radial-gradient(circle_at_80%_20%,rgba(56,189,248,0.1),transparent_30%)]" />
      <div className="relative mx-auto flex min-h-screen w-full max-w-5xl flex-col px-6 py-10 sm:px-10">
        <header className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-xl border border-emerald-400/30 bg-emerald-400/10 text-sm font-semibold text-emerald-300">
              nm
            </div>
            <div>
              <p className="text-sm font-semibold tracking-[0.22em] text-slate-200">
                NETMIRROR
              </p>
              <p className="text-xs text-slate-500">
                Replicated object store
              </p>
            </div>
          </div>
          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-400">
            v2.0 · web
          </span>
        </header>

        <section className="mt-16 grid gap-6 lg:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-3xl border border-white/10 bg-white/[0.035] p-8 shadow-2xl shadow-black/30 backdrop-blur">
            <p className="text-xs font-medium uppercase tracking-[0.24em] text-slate-500">
              Gateway status
            </p>

            <div className="mt-6 flex items-center gap-4" aria-live="polite">
              <span className="relative flex h-4 w-4">
                {online ? (
                  <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-60" />
                ) : null}
                <span
                  className={`relative inline-flex h-4 w-4 rounded-full ${
                    checking
                      ? "bg-amber-400 shadow-[0_0_18px_rgba(251,191,36,0.65)]"
                      : online
                        ? "bg-emerald-400 shadow-[0_0_18px_rgba(52,211,153,0.8)]"
                        : "bg-rose-500 shadow-[0_0_18px_rgba(244,63,94,0.65)]"
                  }`}
                />
              </span>
              <div>
                <h1 className="text-3xl font-semibold tracking-tight text-white">
                  {statusLabel}
                </h1>
                <p className="mt-1 text-sm text-slate-400">{statusDetail}</p>
              </div>
            </div>

            <dl className="mt-8 grid gap-4 sm:grid-cols-2">
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <dt className="text-xs uppercase tracking-wider text-slate-500">
                  Endpoint
                </dt>
                <dd className="mt-1 font-mono text-sm text-slate-200">
                  /api/health
                </dd>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <dt className="text-xs uppercase tracking-wider text-slate-500">
                  Last check
                </dt>
                <dd className="mt-1 font-mono text-sm text-slate-200">
                  {lastChecked ? lastChecked.toLocaleTimeString() : "—"}
                </dd>
              </div>
            </dl>

            <div className="mt-8 flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={() => {
                  setState("checking");
                  setRefreshKey((value) => value + 1);
                }}
                className="rounded-full bg-emerald-400 px-5 py-2.5 text-sm font-semibold text-emerald-950 transition-colors hover:bg-emerald-300"
              >
                Check again
              </button>
              <a
                href="/api/objects"
                target="_blank"
                rel="noreferrer"
                className="rounded-full border border-white/15 px-5 py-2.5 text-sm font-medium text-slate-300 transition-colors hover:border-white/30 hover:text-white"
              >
                Raw objects JSON ↗
              </a>
            </div>
          </div>

          <div className="grid gap-4">
            {features.map((feature) => (
              <article
                key={feature.title}
                className="rounded-2xl border border-white/10 bg-white/[0.025] p-5"
              >
                <div className="mb-2 h-1 w-8 rounded-full bg-emerald-400/70" />
                <h2 className="text-sm font-semibold text-slate-100">
                  {feature.title}
                </h2>
                <p className="mt-1 text-sm leading-6 text-slate-400">
                  {feature.body}
                </p>
              </article>
            ))}
          </div>
        </section>

        <footer className="mt-auto pt-10 text-xs text-slate-600">
          netmirror · HTTP/1.1 gateway · 2-of-3 quorum
        </footer>
      </div>
    </main>
  );
}
