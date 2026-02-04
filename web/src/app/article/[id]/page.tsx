import { notFound } from "next/navigation";

export const dynamic = "force-dynamic";

const API_BASE =
  process.env.API_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "http://localhost:8000";

type Article = {
  id: number | string;
  title?: string;
  summary?: string | null;
  category_guess?: string;
  published_at?: string | null;
  created_at?: string | null;
  source_name?: string | null;
  source_url?: string | null;
  url?: string | null;
  content_clean?: string | null;
  entities?: string[];
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

async function getArticle(id: string): Promise<Article | null> {
  const res = await fetch(`${API_BASE}/public/article/${id}`, {
    next: { revalidate: 60 },
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error("Service unavailable");
  return res.json();
}

export default async function ArticlePage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  let article: Article | null = null;
  try {
    article = await getArticle(id);
  } catch {
    article = { id } as Article;
  }

  if (article === null) {
    notFound();
  }

  const timeLabel = formatTime(article.published_at || article.created_at);

  return (
    <div className="mx-auto max-w-3xl space-y-8">
      <header className="space-y-3">
        <div className="flex flex-wrap items-center gap-2 text-xs text-[var(--muted)]">
          <span className="rounded-full border border-[var(--line)] px-2 py-0.5">
            {article?.category_guess || "General"}
          </span>
          <span>{article?.source_name || "Unknown source"}</span>
          <span>•</span>
          <span>{timeLabel}</span>
        </div>
        <h1 className="text-3xl font-semibold text-[var(--ink)]" style={{ fontFamily: "var(--font-fraunces)" }}>
          {article?.title || "Article"}
        </h1>
        {article?.summary && (
          <div className="rounded-2xl border border-[var(--line)] bg-white/90 p-5 text-sm text-[var(--muted)]">
            {article.summary}
          </div>
        )}
      </header>

      {article?.entities && article.entities.length > 0 && (
        <section className="flex flex-wrap gap-2">
          {article.entities.map((ent) => (
            <span
              key={ent}
              className="rounded-full border border-[var(--line)] bg-white px-3 py-1 text-xs text-[var(--muted)]"
            >
              {ent}
            </span>
          ))}
        </section>
      )}

      <section className="rounded-2xl border border-[var(--line)] bg-white/90 p-6 text-sm leading-7 text-[var(--ink)]">
        {article?.content_clean ? (
          <p>{article.content_clean}</p>
        ) : (
          <p className="text-[var(--muted)]">
            Service temporarily unavailable. Please try again later.
          </p>
        )}
      </section>

      <section className="flex flex-wrap gap-3">
        {article?.url && (
          <a
            className="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs font-semibold text-[var(--ink)]"
            href={article.url}
            target="_blank"
            rel="noreferrer"
          >
            Open source →
          </a>
        )}
        {article?.source_url && (
          <a
            className="rounded-full border border-[var(--line)] bg-white px-4 py-2 text-xs font-semibold text-[var(--ink)]"
            href={article.source_url}
            target="_blank"
            rel="noreferrer"
          >
            Source site →
          </a>
        )}
      </section>
    </div>
  );
}
