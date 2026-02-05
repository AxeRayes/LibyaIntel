import type { NextConfig } from "next";

const API_ORIGIN_RAW =
  process.env.API_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "http://127.0.0.1:8000";

const API_ORIGIN = API_ORIGIN_RAW.replace(/\/$/, "");

const nextConfig: NextConfig = {
  output: "standalone",
  reactCompiler: true,
  async rewrites() {
    return [
      { source: "/api/:path*", destination: `${API_ORIGIN}/api/:path*` },
      { source: "/private/:path*", destination: `${API_ORIGIN}/private/:path*` },
      { source: "/public/:path*", destination: `${API_ORIGIN}/public/:path*` },
      { source: "/health", destination: `${API_ORIGIN}/health` },
    ];
  },
};

export default nextConfig;
