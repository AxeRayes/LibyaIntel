-- Click tracking for alerts
CREATE TABLE IF NOT EXISTS public.alert_clicks (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  user_id uuid NOT NULL,
  delivery_id bigint NOT NULL,
  destination_url text NOT NULL,
  channel text NOT NULL DEFAULT 'email',
  user_agent text NULL,
  ip_hash text NULL
);

CREATE INDEX IF NOT EXISTS alert_clicks_delivery_idx
  ON public.alert_clicks (delivery_id, created_at DESC);

CREATE INDEX IF NOT EXISTS alert_clicks_user_idx
  ON public.alert_clicks (user_id, created_at DESC);
