"use client";

import { useState } from "react";

import FileManager from "@/components/FileManager";
import Upload from "@/components/Upload";

export default function FilesPage() {
  const [filesVersion, setFilesVersion] = useState(0);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-zinc-50">
          File Manager
        </h1>
        <p className="mt-1 text-sm text-zinc-400">
          Browse, download, and manage replicated objects.
        </p>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.7fr)_minmax(320px,0.8fr)]">
        <FileManager refreshKey={filesVersion} />
        <div id="upload" className="scroll-mt-24">
          <Upload
            onUploaded={() => setFilesVersion((value) => value + 1)}
          />
        </div>
      </div>
    </div>
  );
}
