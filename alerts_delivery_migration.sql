-- Alerts delivery reliability migration (LibyaIntel)

-- Cursor table: tracks per-alert progress
CREATE TABLE IF NOT EXISTS public.alert_delivery_cursors (
  alert_id bigint PRIMARY KEY REFERENCES public.alerts(id) ON DELETE CASCADE,
  last_ts timestamptz NOT NULL DEFAULT 'epoch',
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Per-user alert preferences
CREATE TABLE IF NOT EXISTS public.user_alert_prefs (
  user_id uuid PRIMARY KEY,
  dedupe_window_sec integer NOT NULL DEFAULT 21600,
  immediate_priorities text[] NOT NULL DEFAULT ARRAY['P0'],
  digest_priorities text[] NOT NULL DEFAULT ARRAY['P1','P2'],
  priority_categories text[] NULL,
  digest_schedule text NOT NULL DEFAULT 'daily',
  channels_enabled text[] NOT NULL DEFAULT ARRAY['email'],
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Retry fields on deliveries
ALTER TABLE public.alert_deliveries
  ADD COLUMN IF NOT EXISTS queued_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS queued_at_is_estimated boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz,
  ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();

-- Backfill queued_at for existing rows (best-effort, no now() footgun)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'alert_deliveries'
      AND column_name = 'created_at'
  ) THEN
    EXECUTE $q$
      UPDATE public.alert_deliveries
      SET queued_at = COALESCE(queued_at, created_at, last_attempt_at, delivered_at),
          queued_at_is_estimated = true
      WHERE queued_at IS NULL
    $q$;
  ELSE
    EXECUTE $q$
      UPDATE public.alert_deliveries
      SET queued_at = COALESCE(queued_at, last_attempt_at, delivered_at),
          queued_at_is_estimated = true
      WHERE queued_at IS NULL
    $q$;
  END IF;
END $$;

-- Mark truly unknown legacy rows explicitly
UPDATE public.alert_deliveries
SET queued_at = 'epoch',
    queued_at_is_estimated = true
WHERE queued_at IS NULL;

ALTER TABLE public.alert_deliveries
  ALTER COLUMN queued_at SET NOT NULL;

-- Allow PENDING status and default to it
DO $$
DECLARE
  c_name text;
BEGIN
  SELECT conname INTO c_name
  FROM pg_constraint
  WHERE conrelid = 'public.alert_deliveries'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) ILIKE '%status%';
  IF c_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE public.alert_deliveries DROP CONSTRAINT %I', c_name);
  END IF;
END $$;

ALTER TABLE public.alert_deliveries
  ADD CONSTRAINT alert_deliveries_status_check
  CHECK (status IN ('PENDING','SENT','FAILED'));

ALTER TABLE public.alert_deliveries
  ALTER COLUMN status SET DEFAULT 'PENDING';

CREATE INDEX IF NOT EXISTS alert_deliveries_due_idx
  ON public.alert_deliveries (status, next_attempt_at);

CREATE INDEX IF NOT EXISTS alert_deliveries_queued_idx
  ON public.alert_deliveries (status, queued_at);
