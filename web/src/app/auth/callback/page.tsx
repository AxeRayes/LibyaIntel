"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";

export default function AuthCallbackPage() {
  const router = useRouter();

  useEffect(() => {
    const run = async () => {
      const params = new URLSearchParams(window.location.search);
      const code = params.get("code");
      if (code) {
        await supabase.auth.exchangeCodeForSession(code);
      }
      router.replace("/dashboard");
    };
    run();
  }, [router]);

  return (
    <div className="mx-auto max-w-md rounded-2xl border border-[var(--line)] bg-white/90 p-6 text-sm text-[var(--muted)]">
      Signing you in…
    </div>
  );
}
