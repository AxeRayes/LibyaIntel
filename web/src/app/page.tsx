import MarketDashboard, { type MarketQuotesResponse } from "@/components/MarketDashboard";

export const dynamic = "force-dynamic";

const API_BASE =
  process.env.API_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "http://localhost:8000";

type PreviewItem = {
  id: number | string;
  title?: string;
  summary?: string;
  url?: string;
  source?: string;
  category_guess?: string;
  published_at?: string;
  created_at?: string;
};

type PreviewResponse = {
  last_updated: string | null;
  items: PreviewItem[];
  ok: boolean;
};

type ActivityResponse = {
  total_24h: number;
  tenders_24h: number;
  regulations_24h: number;
  high_impact_24h: number;
};

const formatTime = (value?: string | null) => {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(dt);
};

const clamp = (value?: string, max = 280) => {
  if (!value) return "";
  const clean = value.replace(/\s+/g, " ").trim();
  if (clean.length <= max) return clean;
  return `${clean.slice(0, max).trim()}…`;
};

async function getPreview(): Promise<PreviewResponse> {
  const res = await fetch(`${API_BASE}/public/preview?limit=10`, {
    next: { revalidate: 60 },
  });
  if (!res.ok) return { last_updated: null, items: [], ok: false };
  const data = await res.json();
  return { ...data, ok: true };
}

async function getActivity(): Promise<ActivityResponse> {
  const res = await fetch(`${API_BASE}/public/activity`, {
    next: { revalidate: 60 },
  });
  if (!res.ok)
    return { total_24h: 0, tenders_24h: 0, regulations_24h: 0, high_impact_24h: 0 };
  return res.json();
}

async function getMarketQuotes(): Promise<MarketQuotesResponse> {
  try {
    const res = await fetch(`${API_BASE}/api/market/quotes`, {
      next: { revalidate: 300 },
    });
    if (!res.ok) return { as_of: null, items: [], ok: false };
    const data = await res.json();
    return {
      as_of: data.as_of ?? null,
      items: Array.isArray(data.items) ? data.items : [],
      ok: true,
    };
  } catch {
    return { as_of: null, items: [], ok: false };
  }
}

export default async function Page() {
  const [preview, activity, quotes] = await Promise.all([
    getPreview(),
    getActivity(),
    getMarketQuotes(),
  ]);
  const lastUpdated = preview.last_updated
    ? formatTime(preview.last_updated)
    : "—";

  return (
    <div className="space-y-10">
      <section className="grid gap-6 lg:grid-cols-[1.4fr_0.8fr] lg:items-end">
        <div>
          <div className="text-xs uppercase tracking-[0.3em] text-[var(--muted)]">
            LibyaIntel Public Preview
          </div>
          <h1 className="mt-3 text-4xl font-semibold text-[var(--ink)]" style={{ fontFamily: "var(--font-fraunces)" }}>
            Libya market intelligence, distilled daily.
          </h1>
          <p className="mt-4 max-w-2xl text-sm text-[var(--muted)]">
            Verified headlines, rapid summaries, and a snapshot of what changed in the last 24 hours.
            Built for investors, analysts, and decision-makers tracking Libya in real time.
          </p>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/85 p-5 text-sm text-[var(--muted)]">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Last updated
          </div>
          <div className="mt-2 text-lg font-semibold text-[var(--ink)]">{lastUpdated}</div>
          <a
            className="mt-4 inline-flex items-center justify-center rounded-full bg-[var(--accent)] px-5 py-2 text-xs font-semibold text-white"
            href="mailto:hello@libyaintel.com"
          >
            Sign up for alerts →
          </a>
        </div>
      </section>

      <MarketDashboard quotes={quotes} />

      <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Last 24h</div>
          <div className="mt-2 text-2xl font-semibold">{activity.total_24h}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Total articles</div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">High impact</div>
          <div className="mt-2 text-2xl font-semibold">{activity.high_impact_24h}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Disruption signals</div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Tenders</div>
          <div className="mt-2 text-2xl font-semibold">{activity.tenders_24h}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Procurement cues</div>
        </div>
        <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-4">
          <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">Regulations</div>
          <div className="mt-2 text-2xl font-semibold">{activity.regulations_24h}</div>
          <div className="mt-1 text-xs text-[var(--muted)]">Policy changes</div>
        </div>
      </section>

      <section className="space-y-4">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              Latest headlines
            </div>
            <h2 className="mt-2 text-2xl font-semibold" style={{ fontFamily: "var(--font-fraunces)" }}>
              The latest 10 signals
            </h2>
          </div>
          <a
            className="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs font-semibold text-[var(--ink)]"
            href="/dashboard"
          >
            Open dashboard →
          </a>
        </div>

        <div className="grid gap-4">
          {preview.items.length === 0 && (
            <div className="rounded-2xl border border-dashed border-[var(--line)] bg-white/80 p-8 text-sm text-[var(--muted)]">
              {!preview.ok
                ? "Service temporarily unavailable. Please try again shortly."
                : "No updates yet. Ingest sources to populate the public preview."}
            </div>
          )}
          {preview.items.map((item) => {
            const timeLabel = formatTime(item.published_at || item.created_at);
            return (
              <Link
                key={item.id}
                href={`/article/${item.id}`}
                className="group block rounded-2xl border border-[var(--line)] bg-white/90 p-5 shadow-sm transition hover:-translate-y-0.5 hover:border-[var(--accent)]"
              >
                <div className="flex flex-wrap items-center gap-2 text-xs text-[var(--muted)]">
                  <span className="rounded-full border border-[var(--line)] px-2 py-0.5">
                    {item.category_guess || "General"}
                  </span>
                  <span>{item.source || "Unknown source"}</span>
                  <span>•</span>
                  <span>{timeLabel}</span>
                </div>
                <div className="mt-3 text-lg font-semibold text-[var(--ink)]">
                  {item.title || "(no title)"}
                </div>
                <div className="mt-2 text-sm text-[var(--muted)]">
                  {clamp(item.summary, 320)}
                </div>
                <div className="mt-3 inline-flex items-center gap-2 text-sm font-medium text-[var(--accent)]">
                  Read summary →
                </div>
              </Link>
            );
          })}
        </div>
      </section>
    </div>
  );
}
import Link from "next/link";
