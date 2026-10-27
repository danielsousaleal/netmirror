import type { Metadata } from "next";

import AppShell from "@/components/layout/AppShell";

import "./globals.css";

export const metadata: Metadata = {
  title: "Netmirror · Infrastructure Console",
  description: "Distributed object storage console",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" className="h-full">
      <body className="min-h-full bg-zinc-950 text-zinc-400 antialiased">
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
