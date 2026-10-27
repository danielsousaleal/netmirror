import type { NextConfig } from "next";

const gatewayUrl = process.env.NETMIRROR_GATEWAY ?? "http://localhost:8080";

const nextConfig: NextConfig = {
  cacheComponents: true,
  partialPrefetching: true,
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${gatewayUrl}/:path*`,
      },
    ];
  },
};

export default nextConfig;
