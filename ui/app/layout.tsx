import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "netmirror · cluster dashboard",
  description: "Replicated content-addressable object store dashboard",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full bg-[#070a12] text-slate-100">{children}</body>
    </html>
  );
}
