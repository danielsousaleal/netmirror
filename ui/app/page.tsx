"use client";

import Link from "next/link";
import {
  Activity,
  Database,
  Download,
  HardDrive,
  Layers3,
  Loader2,
  Server,
  ShieldAlert,
  ShieldCheck,
  Trash2,
  Upload,
} from "lucide-react";

import { useClusterHealth } from "@/components/health/HealthProvider";
import {
  formatBytes,
  formatPercent,
  formatRelativeTime,
} from "@/lib/format";

function usedBytes(diskTotal: number, diskFree: number): number {
  return Math.max(0, diskTotal - diskFree);
}

export default function OverviewPage() {
  const { state, stats, activity } = useClusterHealth();

  const nodes = stats?.nodes ?? [];
  const onlineNodes = nodes.filter((node) => node.status === "online");
  const allOnline = state === "online";
  const checking = state === "checking";
  const offlineCount = nodes.length - onlineNodes.length;

  const totalCapacity = onlineNodes.reduce(
    (sum, node) => sum + node.disk_total,
    0,
  );
  const usedCapacity = onlineNodes.reduce(
    (sum, node) => sum + usedBytes(node.disk_total, node.disk_free),
    0,
  );
  const usagePercent =
    totalCapacity > 0
      ? Math.min(100, (usedCapacity / totalCapacity) * 100)
      : 0;

  const totalObjects = stats?.total_objects ?? null;
  const totalChunks = stats?.total_chunks ?? null;
  const replicas = nodes.length > 0 ? nodes.length : null;

  const healthLabel = allOnline ? "HEALTH_OK" : "HEALTH_WARN";
  const healthDescription = allOnline
    ? "All services are operational"
    : checking
      ? "Checking cluster health..."
      : stats === null
        ? "Cluster stats unavailable"
        : "One or more nodes are offline";
  const healthDetail = allOnline
    ? `${onlineNodes.length} of ${nodes.length} nodes online`
    : checking
      ? "Contacting /api/cluster/stats..."
      : stats === null
        ? "The stats endpoint did not respond."
        : `${offlineCount} of ${nodes.length} nodes offline`;
  const healthTone = allOnline
    ? "border-emerald-500/20 bg-emerald-500/10 text-emerald-500"
    : "border-amber-500/20 bg-amber-500/10 text-amber-500";
  const healthLabelTone = allOnline ? "text-emerald-500" : "text-amber-500";
  const HealthIcon = checking ? Loader2 : allOnline ? ShieldCheck : ShieldAlert;

  const poolStatus = allOnline ? "Healthy" : checking ? "Checking" : "Degraded";
  const poolTone = allOnline
    ? "border-emerald-500/20 bg-emerald-500/10 text-emerald-500"
    : "border-amber-500/20 bg-amber-500/10 text-amber-500";

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-zinc-50">
            Cluster Overview
          </h1>
          <p className="mt-1 text-sm text-zinc-400">
            Distributed storage health at a glance
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Link
            href="/files#upload"
            className="inline-flex h-9 items-center gap-2 rounded-lg bg-emerald-500 px-3 text-sm font-medium text-zinc-950 transition-colors hover:bg-emerald-400"
          >
            <Upload className="h-4 w-4" />
            Upload File
          </Link>
          <a
            href="#nodes"
            className="inline-flex h-9 items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-900 px-3 text-sm font-medium text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-50"
          >
            <Server className="h-4 w-4" />
            Manage Nodes
          </a>
        </div>
      </div>

      <section className="w-full rounded-xl border border-zinc-800 bg-zinc-900 p-6">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-start gap-4">
            <div
              className={`grid h-12 w-12 shrink-0 place-items-center rounded-xl border ${healthTone}`}
            >
              <HealthIcon
                className={`h-6 w-6 ${checking ? "animate-spin" : ""}`}
              />
            </div>
            <div>
              <p
                className={`text-xs font-semibold tracking-[0.18em] ${healthLabelTone}`}
              >
                {healthLabel}
              </p>
              <h2 className="mt-1 text-lg font-medium text-zinc-50">
                {healthDescription}
              </h2>
              <p className="mt-1 text-sm text-zinc-400">{healthDetail}</p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 sm:gap-6 lg:gap-8">
            <div>
              <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-zinc-500 sm:text-[11px]">
                <HardDrive className="h-3.5 w-3.5" />
                Total Capacity
              </div>
              <p className="mt-1.5 text-base font-semibold text-zinc-50 sm:text-lg">
                {stats === null ? "—" : formatBytes(totalCapacity)}
              </p>
            </div>
            <div>
              <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-zinc-500 sm:text-[11px]">
                <Database className="h-3.5 w-3.5" />
                Used
              </div>
              <p className="mt-1.5 text-base font-semibold text-zinc-50 sm:text-lg">
                {stats === null ? "—" : formatBytes(usedCapacity)}
                <span className="ml-1 text-xs font-normal text-zinc-500">
                  / {stats === null ? "—" : formatPercent(usagePercent)}
                </span>
              </p>
            </div>
            <div>
              <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-zinc-500 sm:text-[11px]">
                <Layers3 className="h-3.5 w-3.5" />
                Objects
              </div>
              <p className="mt-1.5 text-base font-semibold text-zinc-50 sm:text-lg">
                {totalObjects === null
                  ? "—"
                  : totalObjects.toLocaleString("en-US")}
              </p>
            </div>
            <div>
              <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-zinc-500 sm:text-[11px]">
                <Server className="h-3.5 w-3.5" />
                Replicas
              </div>
              <p className="mt-1.5 text-base font-semibold text-zinc-50 sm:text-lg">
                {replicas === null ? "—" : `${replicas}x`}
              </p>
            </div>
          </div>
        </div>

        <div className="mt-6">
          <div className="h-1.5 overflow-hidden rounded-full bg-zinc-800">
            <div
              className="h-full rounded-full bg-emerald-500 transition-[width]"
              style={{ width: `${stats === null ? 0 : usagePercent}%` }}
            />
          </div>
          <div className="mt-2 flex items-center justify-between text-[11px] text-zinc-500">
            <span>
              {stats === null ? "—" : `${formatBytes(usedCapacity)} used`}
            </span>
            <span>
              {stats === null
                ? "—"
                : `${formatPercent(usagePercent)} of ${formatBytes(totalCapacity)}`}
            </span>
          </div>
        </div>
      </section>

      <div className="grid gap-6 lg:grid-cols-2">
        <section
          id="nodes"
          className="scroll-mt-24 overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900"
        >
          <div className="flex items-center justify-between border-b border-zinc-800 px-5 py-4">
            <div>
              <h2 className="text-sm font-semibold text-zinc-50">
                Storage Nodes
              </h2>
              <p className="mt-1 text-xs text-zinc-500">Replication members</p>
            </div>
            <span className="inline-flex items-center gap-1.5 rounded-full border border-zinc-800 bg-zinc-800/50 px-2.5 py-1 text-[11px] text-zinc-400">
              <Server className="h-3 w-3" />
              {stats === null
                ? "—"
                : `${onlineNodes.length}/${nodes.length} online`}
            </span>
          </div>

          {stats === null ? (
            <div className="px-5 py-12 text-center text-sm text-zinc-500">
              {checking
                ? "Loading cluster stats..."
                : "Unable to load cluster stats. Use Refresh to retry."}
            </div>
          ) : nodes.length === 0 ? (
            <div className="px-5 py-12 text-center text-sm text-zinc-500">
              No nodes reported.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[540px] text-left text-sm">
                <thead>
                  <tr className="border-b border-zinc-800 text-[11px] uppercase tracking-wider text-zinc-500">
                    <th className="px-5 py-3 font-medium">Node</th>
                    <th className="px-5 py-3 font-medium">Objects</th>
                    <th className="px-5 py-3 font-medium">Disk Usage</th>
                    <th className="px-5 py-3 text-right font-medium">
                      Status
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-zinc-800">
                  {nodes.map((node) => {
                    const onlineNode = node.status === "online";
                    const diskUsed = usedBytes(
                      node.disk_total,
                      node.disk_free,
                    );
                    const diskPercent =
                      node.disk_total > 0
                        ? Math.min(
                            100,
                            (diskUsed / node.disk_total) * 100,
                          )
                        : 0;
                    const nodeBadge = onlineNode
                      ? "border-emerald-500/20 bg-emerald-500/10 text-emerald-500"
                      : "border-red-500/20 bg-red-500/10 text-red-500";
                    const nodeDot = onlineNode
                      ? "bg-emerald-500"
                      : "bg-red-500";

                    return (
                      <tr key={node.id} className="hover:bg-zinc-800/30">
                        <td className="px-5 py-4">
                          <div className="flex items-center gap-2">
                            <Server className="h-4 w-4 shrink-0 text-zinc-500" />
                            <div className="min-w-0">
                              <p
                                className="truncate font-mono text-[13px] text-zinc-200"
                                title={node.id}
                              >
                                {node.id}
                              </p>
                              <p className="text-[11px] text-zinc-500">
                                Storage node
                              </p>
                            </div>
                          </div>
                        </td>
                        <td className="px-5 py-4 text-sm text-zinc-400">
                          {onlineNode
                            ? node.objects.toLocaleString("en-US")
                            : "—"}
                        </td>
                        <td className="px-5 py-4">
                          {onlineNode && node.disk_total > 0 ? (
                            <div className="flex items-center gap-2">
                              <div className="h-1.5 w-24 overflow-hidden rounded-full bg-zinc-800">
                                <div
                                  className="h-full rounded-full bg-emerald-500"
                                  style={{ width: `${diskPercent}%` }}
                                />
                              </div>
                              <span className="text-xs text-zinc-400">
                                {formatPercent(diskPercent)}
                              </span>
                            </div>
                          ) : (
                            <span className="text-xs text-zinc-500">—</span>
                          )}
                        </td>
                        <td className="px-5 py-4 text-right">
                          <span
                            className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-medium ${nodeBadge}`}
                          >
                            <span
                              className={`h-1.5 w-1.5 rounded-full ${nodeDot}`}
                            />
                            {onlineNode ? "Online" : "Offline"}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900">
          <div className="flex items-center justify-between border-b border-zinc-800 px-5 py-4">
            <div>
              <h2 className="text-sm font-semibold text-zinc-50">
                Storage Pools
              </h2>
              <p className="mt-1 text-xs text-zinc-500">
                Capacity allocation
              </p>
            </div>
            <Database className="h-4 w-4 text-zinc-500" />
          </div>

          <div className="p-5">
            <div className="rounded-lg border border-zinc-800 bg-zinc-950/40 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-medium text-zinc-200">
                    Replicated Objects
                  </p>
                  <p className="mt-1 text-xs text-zinc-500">
                    Content-addressed pool · {replicas ?? "—"} replicas
                  </p>
                </div>
                <span
                  className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-medium ${poolTone}`}
                >
                  <span
                    className={`h-1.5 w-1.5 rounded-full ${
                      allOnline ? "bg-emerald-500" : "bg-amber-500"
                    }`}
                  />
                  {poolStatus}
                </span>
              </div>

              <div className="mt-5 flex items-end justify-between text-xs">
                <span className="text-zinc-400">
                  {stats === null ? "—" : `${formatBytes(usedCapacity)} used`}
                </span>
                <span className="text-zinc-500">
                  {stats === null ? "—" : `${formatBytes(totalCapacity)} total`}
                </span>
              </div>
              <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-zinc-800">
                <div
                  className="h-full rounded-full bg-emerald-500 transition-[width]"
                  style={{ width: `${stats === null ? 0 : usagePercent}%` }}
                />
              </div>

              <div className="mt-4 grid grid-cols-3 gap-3 text-center">
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-zinc-500">
                    Objects
                  </p>
                  <p className="mt-1 text-sm font-medium text-zinc-200">
                    {totalObjects === null
                      ? "—"
                      : totalObjects.toLocaleString("en-US")}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-zinc-500">
                    Chunks
                  </p>
                  <p className="mt-1 text-sm font-medium text-zinc-200">
                    {totalChunks === null
                      ? "—"
                      : totalChunks.toLocaleString("en-US")}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-zinc-500">
                    Replication
                  </p>
                  <p className="mt-1 text-sm font-medium text-zinc-200">
                    {replicas === null ? "—" : `${replicas}x`}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </section>
      </div>

      <section className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900">
        <div className="flex items-center justify-between border-b border-zinc-800 px-5 py-4">
          <div>
            <h2 className="text-sm font-semibold text-zinc-50">
              Recent Activity
            </h2>
            <p className="mt-1 text-xs text-zinc-500">Latest cluster events</p>
          </div>
          <Activity className="h-4 w-4 text-zinc-500" />
        </div>
        {activity.length === 0 ? (
          <div className="px-5 py-10 text-center">
            <p className="text-sm text-zinc-500">No recent activity.</p>
            <p className="mt-1 text-xs text-zinc-600">
              Events appear after uploads, deletes, or node status changes.
            </p>
          </div>
        ) : (
          <ul className="max-h-[24rem] divide-y divide-zinc-800 overflow-y-auto">
            {activity.map((event) => {
              const Icon =
                event.type === "upload"
                  ? Upload
                  : event.type === "download"
                    ? Download
                    : event.type === "delete"
                      ? Trash2
                      : event.type.startsWith("node_")
                        ? Server
                        : Activity;
              const tone =
                event.type === "node_offline"
                  ? "border-red-500/20 bg-red-500/10 text-red-500"
                  : event.type === "delete"
                    ? "border-amber-500/20 bg-amber-500/10 text-amber-500"
                    : event.type === "upload" ||
                        event.type === "download" ||
                        event.type === "node_online"
                      ? "border-emerald-500/20 bg-emerald-500/10 text-emerald-500"
                      : "border-zinc-700 bg-zinc-800 text-zinc-400";

              return (
                <li
                  key={event.id}
                  className="flex items-center gap-3 px-5 py-3.5"
                >
                  <div
                    className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg border ${tone}`}
                  >
                    <Icon className="h-4 w-4" />
                  </div>
                  <p className="min-w-0 flex-1 truncate text-sm text-zinc-400">
                    {event.message}
                  </p>
                  <span className="shrink-0 text-[11px] text-zinc-500">
                    {formatRelativeTime(event.timestamp)}
                  </span>
                </li>
              );
            })}
          </ul>
        )}
      </section>
    </div>
  );
}
