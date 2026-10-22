import type { NextConfig } from "next";

const apiOrigin = process.env.API_ORIGIN;

const nextConfig: NextConfig = {
  output: "standalone",
  async rewrites() {
    if (!apiOrigin) {
      return [];
    }
    return [{ source: "/api/:path*", destination: `${apiOrigin}/api/:path*` }];
  },
};

export default nextConfig;
