CREATE TABLE IF NOT EXISTS public.fetch_queue (
  id bigserial PRIMARY KEY,
  source_id text NOT NULL,
  url text NOT NULL,
  reason text NOT NULL,
  status text NOT NULL DEFAULT 'queued',
  attempts int NOT NULL DEFAULT 0,
  next_run_at timestamptz NOT NULL DEFAULT now(),
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS fetch_queue_unique_active
ON public.fetch_queue (source_id, url, reason)
WHERE status IN ('queued','running');

CREATE INDEX IF NOT EXISTS fetch_queue_next_run
ON public.fetch_queue (status, next_run_at);

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fetch_queue_updated_at ON public.fetch_queue;
CREATE TRIGGER trg_fetch_queue_updated_at
BEFORE UPDATE ON public.fetch_queue
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
