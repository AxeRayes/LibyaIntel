CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenders (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source text NOT NULL,
  buyer text NOT NULL,
  title text,
  summary text,
  publish_date date,
  deadline_date date,
  status text,
  procurement_type text,
  sector text,
  country text DEFAULT 'Libya',
  language text,
  url text UNIQUE NOT NULL,
  attachments_count int DEFAULT 0,
  pdf_text text,
  confidence_score float DEFAULT 0.0,
  raw_article_id uuid,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenders_deadline ON tenders(deadline_date);
CREATE INDEX IF NOT EXISTS idx_tenders_source ON tenders(source);
CREATE INDEX IF NOT EXISTS idx_tenders_sector ON tenders(sector);
