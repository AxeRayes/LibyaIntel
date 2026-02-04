-- Prevent duplicate sends and keep an audit trail
CREATE TABLE IF NOT EXISTS public.alert_deliveries (
  id bigserial PRIMARY KEY,
  alert_id bigint NOT NULL REFERENCES public.alerts(id) ON DELETE CASCADE,
  user_id uuid NOT NULL,
  article_id bigint NOT NULL,
  channel text NOT NULL CHECK (channel IN ('email','telegram')),
  created_at timestamptz NOT NULL DEFAULT now(),
  queued_at timestamptz NOT NULL DEFAULT now(),
  queued_at_is_estimated boolean NOT NULL DEFAULT false,
  dedupe_key text NOT NULL,
  dedupe_group text NULL,
  normalized_url text NULL,
  priority text NOT NULL DEFAULT 'P2',
  delivered_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL DEFAULT 'SENT' CHECK (status IN ('SENT','FAILED')),
  error text NULL
);

-- Never send the same article twice for the same alert+channel
CREATE UNIQUE INDEX IF NOT EXISTS alert_deliveries_uniq
  ON public.alert_deliveries (alert_id, article_id, channel);

CREATE INDEX IF NOT EXISTS alert_deliveries_user_idx
  ON public.alert_deliveries (user_id, delivered_at DESC);

CREATE INDEX IF NOT EXISTS alert_deliveries_alert_idx
  ON public.alert_deliveries (alert_id, delivered_at DESC);

CREATE INDEX IF NOT EXISTS alert_deliveries_dedupe_recent_idx
  ON public.alert_deliveries (user_id, channel, dedupe_key, created_at DESC);

CREATE INDEX IF NOT EXISTS alert_deliveries_runnable_idx
  ON public.alert_deliveries (status, next_attempt_at)
  WHERE status IN ('PENDING','FAILED');

CREATE INDEX IF NOT EXISTS alert_deliveries_priority_runnable_idx
  ON public.alert_deliveries (priority, status, next_attempt_at);

CREATE INDEX IF NOT EXISTS alert_deliveries_queued_idx
  ON public.alert_deliveries (status, queued_at);
