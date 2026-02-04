-- Alert deliveries dedupe + priority columns
ALTER TABLE public.alert_deliveries
  ADD COLUMN IF NOT EXISTS created_at timestamptz,
  ADD COLUMN IF NOT EXISTS dedupe_key text,
  ADD COLUMN IF NOT EXISTS dedupe_group text,
  ADD COLUMN IF NOT EXISTS normalized_url text,
  ADD COLUMN IF NOT EXISTS priority text NOT NULL DEFAULT 'P2';

-- Backfill created_at for existing rows
UPDATE public.alert_deliveries
SET created_at = COALESCE(created_at, delivered_at, queued_at, now())
WHERE created_at IS NULL;

ALTER TABLE public.alert_deliveries
  ALTER COLUMN created_at SET DEFAULT now(),
  ALTER COLUMN created_at SET NOT NULL;

-- Backfill dedupe fields (best-effort)
UPDATE public.alert_deliveries d
SET normalized_url = COALESCE(d.normalized_url, a.url),
    dedupe_key = COALESCE(
      d.dedupe_key,
      a.url,
      a.source_name || '|' || a.title,
      'unknown:' || d.article_id::text
    ),
    dedupe_group = COALESCE(d.dedupe_group, d.dedupe_key),
    priority = COALESCE(d.priority, 'P2')
FROM public.articles a
WHERE d.article_id = a.id;

UPDATE public.alert_deliveries
SET dedupe_key = COALESCE(dedupe_key, 'unknown:' || article_id::text),
    dedupe_group = COALESCE(dedupe_group, dedupe_key),
    priority = COALESCE(priority, 'P2')
WHERE dedupe_key IS NULL OR dedupe_group IS NULL;

ALTER TABLE public.alert_deliveries
  ALTER COLUMN dedupe_key SET NOT NULL;

CREATE INDEX IF NOT EXISTS alert_deliveries_dedupe_recent_idx
  ON public.alert_deliveries (user_id, channel, dedupe_key, created_at DESC);

CREATE INDEX IF NOT EXISTS alert_deliveries_runnable_idx
  ON public.alert_deliveries (status, next_attempt_at)
  WHERE status IN ('PENDING','FAILED');

CREATE INDEX IF NOT EXISTS alert_deliveries_priority_runnable_idx
  ON public.alert_deliveries (priority, status, next_attempt_at);
