-- LibyaIntel: full-text search setup
-- Safe to run repeatedly.

ALTER TABLE public.articles
  ADD COLUMN IF NOT EXISTS search_tsv tsvector
  GENERATED ALWAYS AS (
    to_tsvector(
      'simple',
      coalesce(title, '') || ' ' ||
      coalesce(summary, '') || ' ' ||
      coalesce(content, '') || ' ' ||
      coalesce(translated_content, '')
    )
  ) STORED;

CREATE INDEX CONCURRENTLY IF NOT EXISTS articles_search_tsv_idx
  ON public.articles USING GIN (search_tsv);

CREATE OR REPLACE FUNCTION public.search_articles(
  q text,
  days int,
  category_filter text,
  source_filter text,
  limit_count int
)
RETURNS TABLE (
  id bigint,
  title text,
  summary text,
  url text,
  source text,
  source_name text,
  category text,
  published_at timestamptz,
  created_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    a.id,
    a.title,
    a.summary,
    a.url,
    a.source,
    a.source_name,
    a.category,
    a.published_at,
    a.created_at
  FROM public.articles a
  WHERE
    COALESCE(a.published_at, a.created_at) >= now() - make_interval(days => GREATEST(days, 1))
    AND (category_filter IS NULL OR a.category = category_filter)
    AND (
      source_filter IS NULL OR
      a.source ILIKE source_filter OR
      a.source_name ILIKE source_filter
    )
    AND (
      q IS NULL OR q = '' OR
      a.search_tsv @@ websearch_to_tsquery('simple', q)
    )
  ORDER BY
    CASE
      WHEN q IS NULL OR q = '' THEN COALESCE(a.published_at, a.created_at)
      ELSE NULL
    END DESC,
    CASE
      WHEN q IS NOT NULL AND q <> '' THEN ts_rank_cd(a.search_tsv, websearch_to_tsquery('simple', q))
      ELSE NULL
    END DESC,
    COALESCE(a.published_at, a.created_at) DESC
  LIMIT limit_count;
$$;
