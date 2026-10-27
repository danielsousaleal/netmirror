import type { Metadata } from "next";
import { Settings } from "lucide-react";

export const metadata: Metadata = {
  title: "Settings · Netmirror",
};

const settings = [
  { label: "Gateway endpoint", value: "http://localhost:8080" },
  { label: "Cluster stats endpoint", value: "/api/cluster/stats" },
  { label: "Activity endpoint", value: "/api/cluster/activity" },
  { label: "Object uploads", value: "direct to gateway" },
  { label: "Object downloads", value: "direct to gateway" },
  { label: "Stats polling", value: "5 seconds" },
  { label: "Theme", value: "Dark" },
  { label: "Replication factor", value: "3 replicas" },
  { label: "Netmirror version", value: "v2.0.0" },
];

export default function SettingsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-zinc-50">
          Settings
        </h1>
        <p className="mt-1 text-sm text-zinc-400">
          Console and cluster configuration.
        </p>
      </div>

      <section className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900">
        <div className="flex items-center gap-3 border-b border-zinc-800 px-5 py-4">
          <span className="grid h-8 w-8 place-items-center rounded-lg border border-zinc-800 bg-zinc-800/50 text-zinc-400">
            <Settings className="h-4 w-4" />
          </span>
          <div>
            <h2 className="text-sm font-semibold text-zinc-50">
              Console configuration
            </h2>
            <p className="mt-1 text-xs text-zinc-500">
              Current environment values
            </p>
          </div>
        </div>

        <dl className="divide-y divide-zinc-800">
          {settings.map((setting) => (
            <div
              key={setting.label}
              className="flex items-center justify-between gap-4 px-5 py-3.5"
            >
              <dt className="text-sm text-zinc-400">{setting.label}</dt>
              <dd className="text-right font-mono text-xs text-zinc-200">
                {setting.value}
              </dd>
            </div>
          ))}
        </dl>
      </section>
    </div>
  );
}
