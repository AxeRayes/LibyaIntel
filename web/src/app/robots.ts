import type { MetadataRoute } from "next";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: ["/", "/article/"],
        disallow: ["/dashboard", "/reports"],
      },
    ],
  };
}
