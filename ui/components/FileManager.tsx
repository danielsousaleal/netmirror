"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Download,
  FileText,
  Loader2,
  RefreshCw,
  Search,
  Trash2,
} from "lucide-react";

import { REFRESH_EVENT } from "@/lib/events";
import { objectDownloadUrl } from "@/lib/gateway";

type FileManagerProps = {
  refreshKey?: number;
};

type ObjectsPayload = {
  objects?: unknown;
};

function errorMessage(cause: unknown): string {
  return cause instanceof Error ? cause.message : "unknown error";
}

export default function FileManager({ refreshKey = 0 }: FileManagerProps) {
  const [files, setFiles] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deletingKey, setDeletingKey] = useState<string | null>(null);
  const [reloadToken, setReloadToken] = useState(0);
  const [query, setQuery] = useState("");

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    const load = async () => {
      try {
        const response = await fetch("/api/objects", {
          cache: "no-store",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = (await response.json()) as ObjectsPayload;
        if (!Array.isArray(payload.objects)) {
          throw new Error("Malformed object list");
        }
        const keys = payload.objects
          .filter((value): value is string => typeof value === "string")
          .sort();
        if (!cancelled) {
          setFiles(keys);
          setError(null);
        }
      } catch {
        if (!cancelled) {
          setError("Could not load the object list.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [refreshKey, reloadToken]);

  useEffect(() => {
    const handleRefresh = () => {
      setLoading(true);
      setError(null);
      setDeleteError(null);
      setReloadToken((value) => value + 1);
    };
    window.addEventListener(REFRESH_EVENT, handleRefresh);
    return () => window.removeEventListener(REFRESH_EVENT, handleRefresh);
  }, []);

  const filteredFiles = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) {
      return files;
    }
    return files.filter((key) => key.toLowerCase().includes(normalized));
  }, [files, query]);

  const refresh = () => {
    setLoading(true);
    setError(null);
    setDeleteError(null);
    setReloadToken((value) => value + 1);
  };

  const deleteObject = async (key: string) => {
    if (!window.confirm(`Delete "${key}"? This cannot be undone.`)) {
      return;
    }
    setDeletingKey(key);
    setDeleteError(null);
    try {
      const response = await fetch(`/api/obj/${encodeURIComponent(key)}`, {
        method: "DELETE",
      });
      if (response.status !== 204 && response.status !== 404) {
        let detail = "";
        try {
          const payload = (await response.json()) as { message?: string };
          detail = payload.message ?? "";
        } catch {
          // The gateway can close without a JSON body on transport errors.
        }
        throw new Error(
          `HTTP ${response.status}${detail ? ` — ${detail}` : ""}`,
        );
      }
      setFiles((current) => current.filter((item) => item !== key));
      setReloadToken((value) => value + 1);
    } catch (cause) {
      setDeleteError(`Could not delete ${key}: ${errorMessage(cause)}`);
    } finally {
      setDeletingKey(null);
    }
  };

  const countLabel = query.trim()
    ? `${filteredFiles.length} of ${files.length}`
    : `${files.length} object${files.length === 1 ? "" : "s"}`;

  return (
    <section className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-zinc-800 px-5 py-4">
        <div className="flex items-baseline gap-2">
          <h2 className="text-sm font-semibold text-zinc-50">Objects</h2>
          <span className="text-xs text-zinc-500">{countLabel}</span>
        </div>

        <div className="flex items-center gap-1.5">
          <div className="relative">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-500" />
            <input
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search"
              aria-label="Search objects"
              className="h-8 w-36 rounded-lg border border-zinc-800 bg-zinc-950/40 pl-8 pr-2 text-xs text-zinc-200 outline-none transition-colors placeholder:text-zinc-600 focus:border-emerald-500/50 sm:w-44"
            />
          </div>
          <button
            type="button"
            onClick={refresh}
            aria-label="Refresh objects"
            title="Refresh objects"
            className="grid h-8 w-8 place-items-center rounded-lg border border-zinc-800 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-50"
          >
            <RefreshCw
              className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`}
            />
          </button>
        </div>
      </div>

      {error ? (
        <div className="flex items-center justify-between gap-3 border-b border-zinc-800 px-5 py-3 text-xs text-red-500">
          <span>Could not load objects.</span>
          <button
            type="button"
            onClick={refresh}
            className="font-medium text-red-400 transition-colors hover:text-red-300"
          >
            Retry
          </button>
        </div>
      ) : null}

      {deleteError ? (
        <div className="border-b border-zinc-800 px-5 py-3 text-xs text-red-500">
          {deleteError}
        </div>
      ) : null}

      {loading && files.length === 0 ? (
        <div className="space-y-2 p-5">
          {[0, 1, 2].map((row) => (
            <div
              key={row}
              className="h-10 animate-pulse rounded-lg bg-zinc-800/50"
            />
          ))}
        </div>
      ) : files.length === 0 ? (
        <div className="px-5 py-12 text-center text-sm text-zinc-500">
          No objects yet. Upload a file to get started.
        </div>
      ) : filteredFiles.length === 0 ? (
        <div className="px-5 py-12 text-center text-sm text-zinc-500">
          No objects match &ldquo;{query.trim()}&rdquo;.
        </div>
      ) : (
        <div className="max-h-[28rem] overflow-y-auto">
          {filteredFiles.map((key) => (
            <div
              key={key}
              className="flex items-center justify-between gap-3 border-b border-zinc-800 px-5 py-3 last:border-0 hover:bg-zinc-800/30"
            >
              <div className="flex min-w-0 items-center gap-2.5">
                <FileText className="h-4 w-4 shrink-0 text-zinc-500" />
                <span
                  className="truncate font-mono text-[13px] text-zinc-200"
                  title={key}
                >
                  {key}
                </span>
              </div>

              <div className="flex shrink-0 items-center gap-0.5">
                <a
                  href={objectDownloadUrl(key)}
                  download={key}
                  aria-label={`Download ${key}`}
                  title="Download"
                  className="grid h-8 w-8 place-items-center rounded-md text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-50"
                >
                  <Download className="h-3.5 w-3.5" />
                </a>
                <button
                  type="button"
                  onClick={() => void deleteObject(key)}
                  disabled={deletingKey !== null}
                  aria-label={`Delete ${key}`}
                  title="Delete"
                  className="grid h-8 w-8 place-items-center rounded-md text-zinc-400 transition-colors hover:bg-red-500/10 hover:text-red-500 disabled:cursor-wait disabled:opacity-50"
                >
                  {deletingKey === key ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Trash2 className="h-3.5 w-3.5" />
                  )}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
