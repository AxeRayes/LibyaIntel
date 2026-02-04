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
