"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [mode, setMode] = useState<"magic" | "password">("magic");
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setStatus(null);
    try {
      if (!email) {
        setStatus("Enter your email.");
        return;
      }
      if (mode === "password") {
        const { error } = await supabase.auth.signInWithPassword({ email, password });
        if (error) {
          setStatus(error.message);
          return;
        }
        router.push("/dashboard");
        return;
      }
      const { error } = await supabase.auth.signInWithOtp({
        email,
        options: { emailRedirectTo: `${window.location.origin}/auth/callback` },
      });
      if (error) {
        setStatus(error.message);
        return;
      }
      setStatus("Check your email for a magic link.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mx-auto max-w-md space-y-6 rounded-2xl border border-[var(--line)] bg-white/90 p-6 shadow-sm">
      <div>
        <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Welcome back</div>
        <h1 className="mt-2 text-2xl font-semibold" style={{ fontFamily: "var(--font-fraunces)" }}>
          Sign in
        </h1>
      </div>

      <div className="flex gap-2 text-xs">
        {[
          { key: "magic", label: "Magic link" },
          { key: "password", label: "Password" },
        ].map((opt) => (
          <button
            key={opt.key}
            type="button"
            onClick={() => setMode(opt.key as "magic" | "password")}
            className={`rounded-full border px-3 py-1 ${
              mode === opt.key ? "border-[var(--accent)] text-[var(--accent)]" : "border-[var(--line)] text-[var(--muted)]"
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      <form className="space-y-4" onSubmit={onSubmit}>
        <div className="space-y-1">
          <label className="text-xs text-[var(--muted)]">Email</label>
          <input
            className="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
        </div>
        {mode === "password" && (
          <div className="space-y-1">
            <label className="text-xs text-[var(--muted)]">Password</label>
            <input
              className="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-sm"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>
        )}
        {status && <div className="text-xs text-[var(--muted)]">{status}</div>}
        <button
          className="w-full rounded-full bg-[var(--accent)] px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          type="submit"
          disabled={loading}
        >
          {loading ? "Sending..." : "Continue"}
        </button>
      </form>
    </div>
  );
}
