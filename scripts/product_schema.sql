-- LibyaIntel product schema: entities, saved searches, alerts, reports user_id
-- Safe to run repeatedly.

CREATE TABLE IF NOT EXISTS public.entities (
  id bigserial PRIMARY KEY,
  name text NOT NULL,
  type text,
  normalized_name text,
  created_at timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS entities_normalized_name_idx
  ON public.entities (normalized_name);

CREATE TABLE IF NOT EXISTS public.article_entities (
  id bigserial PRIMARY KEY,
  article_id bigint NOT NULL REFERENCES public.articles(id) ON DELETE CASCADE,
  entity_id bigint NOT NULL REFERENCES public.entities(id) ON DELETE CASCADE,
  confidence numeric,
  created_at timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS article_entities_article_entity_idx
  ON public.article_entities (article_id, entity_id);

CREATE INDEX IF NOT EXISTS article_entities_entity_idx
  ON public.article_entities (entity_id);

CREATE TABLE IF NOT EXISTS public.saved_searches (
  id bigserial PRIMARY KEY,
  user_id uuid NOT NULL,
  name text NOT NULL,
  query text,
  days integer DEFAULT 7,
  category text,
  source text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS saved_searches_user_idx
  ON public.saved_searches (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.alerts (
  id bigserial PRIMARY KEY,
  user_id uuid NOT NULL,
  saved_search_id bigint REFERENCES public.saved_searches(id) ON DELETE CASCADE,
  channel text NOT NULL,
  target text NOT NULL,
  active boolean DEFAULT true,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alerts_user_idx
  ON public.alerts (user_id, created_at DESC);

ALTER TABLE public.reports
  ADD COLUMN IF NOT EXISTS user_id uuid;
