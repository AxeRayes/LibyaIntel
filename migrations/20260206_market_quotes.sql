CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS market_quotes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  instrument text NOT NULL CHECK (instrument IN ('USD','EUR','GBP','EGP','TND','XAU','XAG','BRENT','WTI','NG_TTF','NG_HH')),
  rate_type text NOT NULL CHECK (rate_type IN ('official','parallel','spot')),
  quote_currency text NOT NULL CHECK (quote_currency IN ('LYD','USD')),
  value numeric NOT NULL,
  unit text,
  as_of timestamptz NOT NULL,
  source_name text NOT NULL,
  source_url text NOT NULL,
  status text NOT NULL DEFAULT 'ok' CHECK (status IN ('ok','stale','error')),
  fetched_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_market_quotes_key
  ON market_quotes(instrument, rate_type, quote_currency);

CREATE INDEX IF NOT EXISTS idx_market_quotes_as_of_desc
  ON market_quotes(as_of DESC);

