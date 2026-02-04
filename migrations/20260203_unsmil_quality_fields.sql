ALTER TABLE public.feed_items
  ADD COLUMN IF NOT EXISTS content_kind text,
  ADD COLUMN IF NOT EXISTS verification_status text,
  ADD COLUMN IF NOT EXISTS fetch_quality int;

ALTER TABLE public.articles
  ADD COLUMN IF NOT EXISTS content_kind text,
  ADD COLUMN IF NOT EXISTS verification_status text,
  ADD COLUMN IF NOT EXISTS fetch_quality int;

UPDATE public.feed_items
SET content_kind = COALESCE(content_kind, 'full'),
    verification_status = COALESCE(verification_status, 'full'),
    fetch_quality = COALESCE(fetch_quality, 80)
WHERE content IS NOT NULL AND content <> ''
  AND (content_kind IS NULL OR verification_status IS NULL OR fetch_quality IS NULL);

UPDATE public.feed_items
SET content_kind = COALESCE(content_kind, 'title_only'),
    verification_status = COALESCE(verification_status, 'blocked'),
    fetch_quality = COALESCE(fetch_quality, 0)
WHERE (content IS NULL OR content = '') AND (summary IS NULL OR summary = '')
  AND (content_kind IS NULL OR verification_status IS NULL OR fetch_quality IS NULL);

UPDATE public.articles a
SET content_kind = COALESCE(a.content_kind, fi.content_kind),
    verification_status = COALESCE(a.verification_status, fi.verification_status),
    fetch_quality = COALESCE(a.fetch_quality, fi.fetch_quality)
FROM public.feed_items fi
WHERE a.url = fi.url;
