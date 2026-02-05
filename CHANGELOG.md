# Changelog

## 2026-02-01
Impact: Improves alert reliability and prevents missed or oversized deliveries.

Latest Features
- Long-running alerts worker (timer removed) with configurable poll interval (ALERTS_POLL_INTERVAL_SEC)
- Email digest batching with size cap and automatic overflow splitting (ALERTS_MAX_ITEMS_PER_EMAIL)
- Fail-loud delivery logging when max retry attempts are reached (ALERTS_DELIVERY_GIVEUP)
- Crash-safe cursor progression (advance only after queue commits)
- Hardened systemd service
  - dedicated non-login user
  - /opt runtime
  - strict filesystem protections
  - dropped Linux capabilities
- Operations documentation expanded
  - environment variables
  - migration checklist
  - rollback steps
  - smoke tests

## 2026-02-04
Impact: Turns procurement ingestion into a sellable weekly digest product.

Latest Features
- Structured procurement schema (tenders table)
- Tender extraction job with per-source filtering (--source) and Arabic keyword/deadline handling
- LPMA detail-page HTML gating (content-based, production-grade)
- Attachment parsing support (PDF/DOC/DOCX) with fallback gating
- Weekly procurement digest generator (Markdown)
  - "new since last digest" window via last_procurement_digest_at.txt
  - per-buyer grouping + counts
  - attachments_count signal
- systemd services/timers
  - libyaintel-extract-tenders.service + timer
  - libyaintel-procurement-digest.service + timer
- deploy.sh installs migrations and enables timers
