"use client";

import { useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";

export default function CallbackClient() {
  const router = useRouter();
  const sp = useSearchParams();

  useEffect(() => {
    const next = sp.get("next") || "/";
    router.replace(next);
  }, [router, sp]);

  return (
    <div style={{ padding: 24 }}>
      <h1>Signing you in…</h1>
      <p>Please wait.</p>
    </div>
  );
}
