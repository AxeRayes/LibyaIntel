# Sprint: Platform Phase 1 (Foundation + Homepage Dashboard + Concierge Intake)

Implementation order is **non-negotiable**:

- [ ] Ticket 1 (Market Quotes DB + API)
- [ ] Ticket 2 (Market Quotes Fetcher Job + systemd)
- [ ] Ticket 3 (Homepage Market Dashboard Widget)
- [ ] Ticket 4 (Services Hub + Request Support Intake)
- [ ] Ticket 5 (Partner System, internal only)

Global requirements (apply to all tickets)

- [ ] English UI everywhere
- [ ] Every external number includes `source_url` + `as_of`
- [ ] Fail-soft UI: never white-screen
- [ ] Systemd jobs emit **one** final `*_OK` or `*_FAIL` log line

---

## Ticket 1 — Market Quotes DB + API

Goal: Store and serve FX + metals + oil/gas quotes from DB.

Deliverables

- [ ] Migration: `migrations/20260206_market_quotes.sql`
- [ ] Table: `market_quotes`
- [ ] API: `backend/api/main.py` adds `GET /api/market/quotes`

Schema (DB)

- `instrument` (USD, EUR, GBP, EGP, TND, XAU, XAG, BRENT, WTI, NG_TTF, NG_HH)
- `rate_type` (official | parallel | spot)
- `quote_currency` (LYD or USD)
- `value` numeric
- `as_of` timestamptz
- `source_name`, `source_url`
- `status` (ok|stale|error)
- unique index on (`instrument`, `rate_type`, `quote_currency`)

Acceptance criteria

- [ ] Calling `/api/market/quotes` returns latest quotes for all instruments that exist in DB.
- [ ] Response items include `as_of`, `source_url`, `status`.

---

## Ticket 2 — Market Quotes Fetcher Job (CBL + metals + commodities + parallel manual)

Goal: Scheduled ingestion of market data with caching + stale rules.

Deliverables

- [ ] Job: `runner/jobs/fetch_market_quotes.py`
- [ ] systemd unit: `systemd/libyaintel-market-quotes.service`
- [ ] systemd timer: `systemd/libyaintel-market-quotes.timer` (every 30 minutes)

Config support

- [ ] `PARALLEL_FX_MODE=manual` (v1)
- [ ] `/etc/libyaintel/parallel_fx.json` (manual parallel rates)
- [ ] `MARKET_QUOTES_STALE_AFTER_MIN=180` (default 3h)
- [ ] `MARKET_QUOTES_FETCH_TIMEOUT=15`

Behavior

- [ ] Official FX: pulls from CBL (HTTP fetch + parse)
- [ ] Parallel FX: reads manual JSON and upserts
- [ ] Metals: spot XAU/XAG
- [ ] Commodities: Brent/WTI/NG benchmark
- [ ] On fetch error: keeps last values; mark `status=stale` if older than threshold
- [ ] Logs one final line: `MARKET_QUOTES_OK instruments=N updated=N stale=N` (or `MARKET_QUOTES_FAIL ...`)

Acceptance criteria

- [ ] Running job produces `MARKET_QUOTES_OK ...`
- [ ] DB has up-to-date rows (or `stale` if source is down)

---

## Ticket 3 — Homepage Market Dashboard Widget

Goal: Show FX (official + parallel), metals, oil/gas at top of homepage.

Deliverables

- [ ] Component: `web/src/components/MarketDashboard.tsx`
- [ ] Homepage integration: `web/src/app/page.tsx` calls `/api/market/quotes`

UI rules

- [ ] English only
- [ ] Show “Official” and “Parallel (Indicative)” labels
- [ ] Show timestamp + source link
- [ ] Show stale badge when `status != ok`

Acceptance criteria

- [ ] Loads in < 1s after page render
- [ ] No layout shift
- [ ] Mobile responsive
- [ ] If API fails: shows “Data temporarily unavailable” without crashing

---

## Ticket 4 — Services Hub + Request Support Intake

Goal: Convert platform into a base for “get help” via curated partners.

Deliverables

- [ ] Page: `web/src/app/services/page.tsx` (English)
- [ ] Page: `web/src/app/request-support/page.tsx` (single form)
- [ ] API: `backend/api/main.py` adds `POST /api/service-requests`
- [ ] Email notify (Resend): sends to `SUPPORT_INBOX_EMAIL`

Form fields

- [ ] category dropdown: legal, tax, accounting, payroll, EOR/manpower, recruitment, training, consultancy
- [ ] name, company, email, WhatsApp
- [ ] country/city
- [ ] urgency
- [ ] message

Acceptance criteria

- [ ] Submit creates DB row + sends email to `SUPPORT_INBOX_EMAIL`
- [ ] Returns `{request_id}`
- [ ] No PII leakage in logs (log `request_id` only)

---

## Ticket 5 — Partner System (internal only, monetization-ready)

Goal: Store partners + track leads for future yearly fee model.

Deliverables

- [ ] Migration: `migrations/20260206_partners.sql`
- [ ] Tables: `partners`, `service_requests`, `partner_leads`
- [ ] Admin endpoints (v1 simple):
  - [ ] `GET /api/admin/requests`
  - [ ] `POST /api/admin/requests/{id}/assign`

Partner fields (must include)

- [ ] `status` (pending/approved/suspended)
- [ ] `tier` (standard/premium/featured)
- [ ] `annual_fee_usd`, `renewal_date`
- [ ] `categories` array
- [ ] internal notes

Acceptance criteria

- [ ] Can add partner rows (via DB for now; UI later)
- [ ] Can assign partner to request (API)
- [ ] Assignment creates a `partner_leads` record
- [ ] No public partner directory yet

