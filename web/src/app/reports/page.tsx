"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export default function ReportsPage() {
  const router = useRouter();
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [language, setLanguage] = useState("all");
  const [keywords, setKeywords] = useState("");
  const [includeSources, setIncludeSources] = useState(true);
  const [loading, setLoading] = useState(false);
  const [markdown, setMarkdown] = useState("");
  const [saveStatus, setSaveStatus] = useState("");

  useEffect(() => {
    const check = async () => {
      const { data } = await supabase.auth.getSession();
      if (!data.session) {
        router.replace("/login");
        return;
      }
      setAccessToken(data.session.access_token);
      setReady(true);
    };
    check();
  }, [router]);

  const toIso = (value: string) => {
    if (!value) return null;
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  };

  const handleGenerate = async () => {
    if (!accessToken) return;
    setLoading(true);
    setSaveStatus("");
    try {
      const payload = {
        start: toIso(start),
        end: toIso(end),
        language: language === "all" ? null : language,
        keywords: keywords
          .split(",")
          .map((k) => k.trim())
          .filter(Boolean),
        include_sources: includeSources,
        limit: 80,
      };

      const res = await fetch(`${API_BASE}/private/reports/generate`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        throw new Error("Failed to generate report.");
      }

      const data = await res.json();
      setMarkdown(data.markdown || "");
    } catch (error) {
      setMarkdown("Report generation failed. Check the API logs.");
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!markdown || !accessToken) return;
    setSaveStatus("Saving...");
    try {
      const res = await fetch(`${API_BASE}/private/reports/save`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify({
          title: `LibyaIntel Report ${new Date().toISOString().slice(0, 10)}`,
          markdown,
          metadata: { start, end, language, keywords },
        }),
      });

      if (!res.ok) {
        throw new Error("Failed to save report.");
      }

      setSaveStatus("Saved.");
    } catch (error) {
      setSaveStatus("Save failed.");
    }
  };

  if (!ready) {
    return (
      <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-6 text-sm text-[var(--muted)]">
        Checking session…
      </div>
    );
  }

  return (
    <div className="grid gap-8 lg:grid-cols-[360px_1fr]">
      <div className="space-y-6 rounded-2xl border border-[var(--line)] bg-white/90 p-6 shadow-sm">
        <div>
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Report Generator
          </div>
          <h1 className="mt-2 text-2xl font-semibold" style={{ fontFamily: "var(--font-fraunces)" }}>
            Draft Intelligence Brief
          </h1>
        </div>

        <div className="space-y-4 text-sm">
          <label className="block">
            <span className="text-[var(--muted)]">Start</span>
            <input
              type="datetime-local"
              className="mt-2 w-full rounded-xl border border-[var(--line)] px-3 py-2"
              value={start}
              onChange={(event) => setStart(event.target.value)}
            />
          </label>
          <label className="block">
            <span className="text-[var(--muted)]">End</span>
            <input
              type="datetime-local"
              className="mt-2 w-full rounded-xl border border-[var(--line)] px-3 py-2"
              value={end}
              onChange={(event) => setEnd(event.target.value)}
            />
          </label>
          <label className="block">
            <span className="text-[var(--muted)]">Language</span>
            <select
              className="mt-2 w-full rounded-xl border border-[var(--line)] px-3 py-2"
              value={language}
              onChange={(event) => setLanguage(event.target.value)}
            >
              <option value="all">All</option>
              <option value="ar">Arabic</option>
              <option value="en">English</option>
            </select>
          </label>
          <label className="block">
            <span className="text-[var(--muted)]">Keywords (comma separated)</span>
            <input
              type="text"
              className="mt-2 w-full rounded-xl border border-[var(--line)] px-3 py-2"
              value={keywords}
              onChange={(event) => setKeywords(event.target.value)}
              placeholder="oil, elections, FX"
            />
          </label>
          <label className="flex items-center gap-2 text-sm text-[var(--muted)]">
            <input
              type="checkbox"
              checked={includeSources}
              onChange={(event) => setIncludeSources(event.target.checked)}
            />
            Include sources list
          </label>
        </div>

        <div className="flex flex-col gap-2">
          <button
            className="rounded-xl bg-[var(--accent)] px-4 py-2 text-sm font-semibold text-white"
            onClick={handleGenerate}
            disabled={loading}
          >
            {loading ? "Generating..." : "Generate report"}
          </button>
          <button
            className="rounded-xl border border-[var(--line)] bg-white px-4 py-2 text-sm font-semibold text-[var(--ink)]"
            onClick={handleSave}
            disabled={!markdown}
          >
            Save report
          </button>
          {saveStatus && <div className="text-xs text-[var(--muted)]">{saveStatus}</div>}
        </div>
      </div>

      <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-6 shadow-sm">
        <div className="flex items-center justify-between">
          <div className="text-sm font-medium text-[var(--ink)]">Report Preview</div>
          <div className="text-xs text-[var(--muted)]">Markdown</div>
        </div>
        <pre className="mt-4 whitespace-pre-wrap text-sm text-[var(--muted)]">
          {markdown || "Generate a report to preview the markdown."}
        </pre>
      </div>
    </div>
  );
}
