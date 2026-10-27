"use client";

import {
  useRef,
  useState,
  type ChangeEvent,
  type DragEvent,
} from "react";
import {
  AlertCircle,
  CheckCircle2,
  CloudUpload,
  FileText,
  Loader2,
} from "lucide-react";

import { formatBytes } from "@/lib/format";
import { objectUploadUrl } from "@/lib/gateway";

type UploadProps = {
  onUploaded?: () => void;
};

type UploadState = "idle" | "uploading" | "success" | "error";

type SelectedFile = {
  name: string;
  size: number;
  key: string;
};

function normalizeKey(filename: string): string {
  const base = filename.split(/[\\/]/).pop() ?? "";
  const sanitized = base
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/^[-.]+|[-.]+$/g, "")
    .slice(0, 127);
  return sanitized !== "" && sanitized !== "." && sanitized !== ".."
    ? sanitized
    : `upload-${Date.now()}`;
}

export default function Upload({ onUploaded }: UploadProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const dragDepth = useRef(0);
  const [state, setState] = useState<UploadState>("idle");
  const [selected, setSelected] = useState<SelectedFile | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);

  const busy = state === "uploading";

  const upload = async (file: File) => {
    const key = normalizeKey(file.name);
    setSelected({ name: file.name, size: file.size, key });
    setState("uploading");
    setMessage(null);
    try {
      const response = await fetch(objectUploadUrl(key), {
        method: "PUT",
        headers: { "Content-Type": file.type || "application/octet-stream" },
        body: file,
      });
      if (!response.ok) {
        let detail = "";
        try {
          const payload = (await response.json()) as { message?: string };
          detail = payload.message ?? "";
        } catch {
          // Ignore non-JSON error bodies.
        }
        if (response.status === 503) {
          throw new Error(
            "503 — quorum unavailable; at least two nodes must acknowledge the write.",
          );
        }
        throw new Error(
          `HTTP ${response.status}${detail ? ` — ${detail}` : ""}`,
        );
      }
      const payload = (await response.json()) as {
        key?: string;
        size?: number;
      };
      const uploadedKey = payload.key ?? key;
      const uploadedSize =
        typeof payload.size === "number" ? payload.size : file.size;
      setState("success");
      setMessage(
        `Upload complete: ${uploadedKey} (${formatBytes(uploadedSize)})`,
      );
      onUploaded?.();
    } catch (cause) {
      setState("error");
      setMessage(cause instanceof Error ? cause.message : "Upload failed.");
    } finally {
      if (inputRef.current) {
        inputRef.current.value = "";
      }
    }
  };

  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      void upload(file);
    }
  };

  const handleDragEnter = (event: DragEvent<HTMLLabelElement>) => {
    event.preventDefault();
    if (busy) {
      return;
    }
    dragDepth.current += 1;
    setDragging(true);
  };

  const handleDragOver = (event: DragEvent<HTMLLabelElement>) => {
    event.preventDefault();
    if (!busy) {
      event.dataTransfer.dropEffect = "copy";
    }
  };

  const handleDragLeave = (event: DragEvent<HTMLLabelElement>) => {
    event.preventDefault();
    dragDepth.current = Math.max(0, dragDepth.current - 1);
    if (dragDepth.current === 0) {
      setDragging(false);
    }
  };

  const handleDrop = (event: DragEvent<HTMLLabelElement>) => {
    event.preventDefault();
    dragDepth.current = 0;
    setDragging(false);
    if (busy) {
      return;
    }
    const file = event.dataTransfer.files?.[0];
    if (file) {
      void upload(file);
    }
  };

  const dropzoneTone = busy
    ? "pointer-events-none border-zinc-800 bg-zinc-950/20 opacity-60"
    : dragging
      ? "border-emerald-500/50 bg-emerald-500/[0.06]"
      : "border-zinc-800 bg-zinc-950/30 hover:border-emerald-500/40 hover:bg-zinc-800/30";

  return (
    <section
      aria-busy={busy}
      className="rounded-xl border border-zinc-800 bg-zinc-900"
    >
      <div className="border-b border-zinc-800 px-5 py-4">
        <h2 className="text-sm font-semibold text-zinc-50">Upload</h2>
        <p className="mt-1 text-xs text-zinc-500">
          Send a file to the replicated store.
        </p>
      </div>

      <div className="p-5">
        <label
          htmlFor="file-upload"
          onDragEnter={handleDragEnter}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
          className={`flex cursor-pointer flex-col items-center justify-center rounded-lg border border-dashed px-4 py-8 text-center transition-colors ${dropzoneTone}`}
        >
          <input
            ref={inputRef}
            id="file-upload"
            type="file"
            className="sr-only"
            onChange={handleChange}
            disabled={busy}
          />
          {busy ? (
            <Loader2 className="h-5 w-5 animate-spin text-emerald-500" />
          ) : (
            <CloudUpload className="h-5 w-5 text-zinc-500" />
          )}
          <span className="mt-2 text-sm text-zinc-300">
            {busy
              ? "Enviando..."
              : dragging
                ? "Drop to upload"
                : "Drop a file or browse"}
          </span>
          <span className="mt-1 text-xs text-zinc-500">
            {busy
              ? "Waiting for the 2-of-3 quorum."
              : "The key is derived from the file name."}
          </span>
        </label>

        {selected ? (
          <div className="mt-3 min-w-0">
            <div className="flex items-center gap-2 text-xs text-zinc-500">
              <FileText className="h-3.5 w-3.5 shrink-0" />
              <span className="truncate text-zinc-300" title={selected.name}>
                {selected.name}
              </span>
              <span className="shrink-0">· {formatBytes(selected.size)}</span>
            </div>
            <p className="mt-1 truncate font-mono text-[11px] text-zinc-600">
              key: {selected.key}
            </p>
          </div>
        ) : null}

        {message ? (
          <p
            role={state === "error" ? "alert" : "status"}
            aria-live="polite"
            className={`mt-3 flex items-start gap-2 text-xs ${
              state === "success" ? "text-emerald-500" : "text-red-500"
            }`}
          >
            {state === "success" ? (
              <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            ) : (
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            )}
            <span>{message}</span>
          </p>
        ) : null}
      </div>
    </section>
  );
}
