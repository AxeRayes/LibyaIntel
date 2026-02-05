"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabaseClient";

type Activity = {
  total_24h: number;
  high_impact_24h: number;
  tenders_24h: number;
  regulations_24h: number;
};

type SearchItem = {
  id: number | string;
  title?: string;
  summary?: string;
  url?: string;
  source?: string;
  source_name?: string;
  category_guess?: string;
  published_at?: string;
  created_at?: string;
};

type SavedSearch = {
  id: number;
  name: string;
  query?: string | null;
  days?: number | null;
  category?: string | null;
  source?: string | null;
  created_at?: string | null;
};

const formatTime = (value?: string | null) => {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value || "—";
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(dt);
};

export default function DashboardPage() {
  const router = useRouter();
  const [ready, setReady] = useState(false);
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [userEmail, setUserEmail] = useState<string | null>(null);
  const [activity, setActivity] = useState<Activity | null>(null);
  const [items, setItems] = useState<SearchItem[]>([]);
  const [savedSearches, setSavedSearches] = useState<SavedSearch[]>([]);
  const [saveName, setSaveName] = useState("");
  const [alertStatus, setAlertStatus] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [days, setDays] = useState("7");
  const [category, setCategory] = useState("all");
  const [source, setSource] = useState("all");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const check = async () => {
      const sessionRes = await supabase.auth.getSession();
      if (!sessionRes.data.session) {
        router.replace("/login");
        return;
      }
      setAccessToken(sessionRes.data.session.access_token);
      const userRes = await supabase.auth.getUser();
      setUserEmail(userRes.data.user?.email || null);
      setReady(true);
    };
    check();
  }, [router]);

  const fetchActivity = async () => {
    const { data } = await supabase.auth.getSession();
    const token = data.session?.access_token;
    if (!token) return null;
    const res = await fetch(`/private/activity`, {
      cache: "no-store",
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!res.ok) return null;
    return res.json();
  };

  const fetchResults = async () => {
    const { data } = await supabase.auth.getSession();
    const token = data.session?.access_token || accessToken;
    if (!token) return;
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        limit: "50",
        days,
      });
      if (query) params.set("q", query);
      if (category !== "all") params.set("category", category);
      if (source !== "all") params.set("source", source);
      const res = await fetch(`/private/search?${params.toString()}`, {
        cache: "no-store",
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!res.ok) {
        setError("Search failed. Check API connectivity.");
        return;
      }
      const data = await res.json();
      setItems(data.items || []);
    } finally {
      setLoading(false);
    }
  };

  const fetchSavedSearches = async () => {
    if (!accessToken) return;
    const res = await fetch(`/private/saved-searches`, {
      cache: "no-store",
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!res.ok) return;
    const data = await res.json();
    setSavedSearches(data.items || []);
  };

  const handleSaveSearch = async () => {
    if (!accessToken) return;
    if (!saveName.trim()) {
      setError("Name your saved search before saving.");
      return;
    }
    setError(null);
    const payload = {
      name: saveName.trim(),
      query: query || null,
      days: Number(days),
      category: category !== "all" ? category : null,
      source: source !== "all" ? source : null,
    };
    const res = await fetch(`/private/saved-searches`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      setError("Could not save search.");
      return;
    }
    setSaveName("");
    fetchSavedSearches();
  };

  const handleCreateAlert = async (searchId: number) => {
    if (!accessToken || !userEmail) {
      setAlertStatus("No email available for alerts.");
      return;
    }
    setAlertStatus(null);
    const res = await fetch(`/private/alerts`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({
        saved_search_id: searchId,
        channel: "email",
        target: userEmail,
        active: true,
      }),
    });
    if (!res.ok) {
      setAlertStatus("Could not create alert.");
      return;
    }
    setAlertStatus("Alert created.");
  };

  useEffect(() => {
    if (!ready) return;
    fetchActivity().then(setActivity);
    fetchResults();
    fetchSavedSearches();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ready, accessToken]);

  const categories = useMemo(() => {
    const values = new Set<string>();
    items.forEach((item) => {
      if (item.category_guess) values.add(item.category_guess);
    });
    return ["all", ...Array.from(values)];
  }, [items]);

  const sources = useMemo(() => {
    const values = new Set<string>();
    items.forEach((item) => {
      const name = item.source_name || item.source;
      if (name) values.add(name);
    });
    return ["all", ...Array.from(values)];
  }, [items]);

  if (!ready) {
    return (
      <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-6 text-sm text-[var(--muted)]">
        Checking session…
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Intelligence Dashboard
          </div>
          <h1 className="mt-2 text-3xl font-semibold" style={{ fontFamily: "var(--font-fraunces)" }}>
            Today in Libya
          </h1>
        </div>
        <div className="text-xs text-[var(--muted)]">Private view • Logged in</div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Last 24h</div>
          <div className="mt-2 text-2xl font-semibold">{activity?.total_24h ?? "—"}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Total articles</div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">High impact</div>
          <div className="mt-2 text-2xl font-semibold">{activity?.high_impact_24h ?? "—"}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Disruption signals</div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Tenders</div>
          <div className="mt-2 text-2xl font-semibold">{activity?.tenders_24h ?? "—"}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Procurement cues</div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Regulations</div>
          <div className="mt-2 text-2xl font-semibold">{activity?.regulations_24h ?? "—"}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Policy changes</div>
        </div>
      </div>

      <div className="grid gap-4 rounded-2xl border border-[var(--line)] bg-white/90 p-5 lg:grid-cols-[1.3fr_1fr]">
        <div className="space-y-3">
          <div className="text-sm font-semibold text-[var(--ink)]">Search</div>
          <input
            className="w-full rounded-xl border border-[var(--line)] bg-white px-4 py-2 text-sm"
            placeholder="Keywords (title, summary, content)"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="space-y-2">
            <div className="text-xs text-[var(--muted)]">Date range</div>
            <select
              className="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-xs"
              value={days}
              onChange={(e) => setDays(e.target.value)}
            >
              <option value="1">Last 24h</option>
              <option value="7">Last 7d</option>
              <option value="30">Last 30d</option>
            </select>
          </div>
          <div className="space-y-2">
            <div className="text-xs text-[var(--muted)]">Category</div>
            <select
              className="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-xs"
              value={category}
              onChange={(e) => setCategory(e.target.value)}
            >
              {categories.map((c) => (
                <option key={c} value={c}>
                  {c === "all" ? "All" : c}
                </option>
              ))}
            </select>
          </div>
          <div className="space-y-2">
            <div className="text-xs text-[var(--muted)]">Source</div>
            <select
              className="w-full rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-xs"
              value={source}
              onChange={(e) => setSource(e.target.value)}
            >
              {sources.map((s) => (
                <option key={s} value={s}>
                  {s === "all" ? "All" : s}
                </option>
              ))}
            </select>
          </div>
        </div>
        <div className="flex items-center justify-end">
          <div className="flex flex-wrap items-center gap-2">
            <button
              className="rounded-full bg-[var(--accent)] px-4 py-2 text-xs font-semibold text-white disabled:opacity-60"
              onClick={fetchResults}
              disabled={loading}
            >
              {loading ? "Searching..." : "Run search"}
            </button>
            <button
              className="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs font-semibold text-[var(--ink)]"
              onClick={handleSaveSearch}
            >
              Save search
            </button>
          </div>
        </div>
      </div>

      <div className="grid gap-4 rounded-2xl border border-[var(--line)] bg-white/90 p-5 lg:grid-cols-[1.2fr_1fr]">
        <div className="space-y-2">
          <div className="text-sm font-semibold text-[var(--ink)]">Saved searches</div>
          <div className="text-xs text-[var(--muted)]">Reuse your filters and alerts later.</div>
          <input
            className="mt-2 w-full rounded-xl border border-[var(--line)] bg-white px-4 py-2 text-sm"
            placeholder="Name this search"
            value={saveName}
            onChange={(e) => setSaveName(e.target.value)}
          />
        </div>
        <div className="space-y-2">
          {savedSearches.length === 0 && (
            <div className="text-xs text-[var(--muted)]">No saved searches yet.</div>
          )}
          {savedSearches.map((s) => (
            <div key={s.id} className="rounded-xl border border-[var(--line)] bg-white px-3 py-2 text-xs">
              <div className="font-semibold text-[var(--ink)]">{s.name}</div>
              <div className="mt-1 text-[var(--muted)]">
                {s.query || "No keyword"} · {s.days || 7}d · {s.category || "All"} · {s.source || "All"}
              </div>
              <button
                className="mt-2 rounded-full border border-[var(--line)] bg-white px-3 py-1 text-[11px] font-semibold text-[var(--ink)]"
                onClick={() => handleCreateAlert(s.id)}
              >
                Create email alert
              </button>
            </div>
          ))}
          {alertStatus && <div className="text-[11px] text-[var(--muted)]">{alertStatus}</div>}
        </div>
      </div>

      {error && (
        <div className="rounded-2xl border border-dashed border-[var(--line)] bg-white/80 p-4 text-sm text-[var(--muted)]">
          {error}
        </div>
      )}

      <div className="space-y-3">
        <div className="flex items-center justify-between text-xs text-[var(--muted)]">
          <span>Latest results</span>
          <span>{items.length} items</span>
        </div>
        <div className="grid gap-4">
          {items.map((item) => {
            const timeLabel = formatTime(item.published_at || item.created_at);
            const sourceLabel = item.source_name || item.source || "Unknown source";
            return (
              <Link
                key={item.id}
                href={`/article/${item.id}`}
                className="block rounded-2xl border border-[var(--line)] bg-white/90 p-5 shadow-sm transition hover:-translate-y-0.5 hover:border-[var(--accent)]"
              >
                <div className="flex flex-wrap items-center gap-2 text-xs text-[var(--muted)]">
                  <span className="rounded-full border border-[var(--line)] px-2 py-0.5">
                    {item.category_guess || "General"}
                  </span>
                  <span>{sourceLabel}</span>
                  <span>•</span>
                  <span>{timeLabel}</span>
                </div>
                <div className="mt-3 text-lg font-semibold text-[var(--ink)]">
                  {item.title || "(no title)"}
                </div>
                <div className="mt-2 text-sm text-[var(--muted)]">
                  {item.summary || ""}
                </div>
              </Link>
            );
          })}
        </div>
      </div>
    </div>
  );
}
