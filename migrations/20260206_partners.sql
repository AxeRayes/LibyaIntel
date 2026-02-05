-- Concierge + partners (internal) foundation.
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS partners (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_name text NOT NULL,
  categories text[] NOT NULL DEFAULT '{}'::text[],
  city text,
  contact_name text,
  email text,
  phone text,
  website text,
  status text NOT NULL DEFAULT 'pending',
  tier text NOT NULL DEFAULT 'standard',
  is_public boolean NOT NULL DEFAULT false,
  annual_fee_usd numeric,
  renewal_date date,
  notes_internal text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS service_requests (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  category text NOT NULL,
  company_name text,
  contact_name text,
  email text,
  whatsapp text,
  country text,
  city text,
  urgency text NOT NULL DEFAULT 'normal',
  message text NOT NULL,
  status text NOT NULL DEFAULT 'new',
  assigned_partner_id uuid REFERENCES partners(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partner_leads (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  partner_id uuid NOT NULL REFERENCES partners(id),
  request_id uuid NOT NULL REFERENCES service_requests(id),
  sent_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL DEFAULT 'sent',
  outcome_notes text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_service_requests_status_created_at
  ON service_requests (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_partner_leads_partner_sent_at
  ON partner_leads (partner_id, sent_at DESC);

