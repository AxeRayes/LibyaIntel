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
