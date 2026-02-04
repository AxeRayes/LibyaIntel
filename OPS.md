# Ops notes (LibyaIntel)

## Units and timers
- `libyaintel-summarize.service` (oneshot)
- `libyaintel-summarize.timer` (every 5 minutes, clock-aligned)
- `libyaintel-page-ingest.service` (oneshot)
- `libyaintel-page-ingest.timer` (every 15 minutes, clock-aligned)
- `libyaintel-alerts.service` (long-running alerts worker)

## Locks and overlap protection
- Summarize lock: `/run/libyaintel/summarize.lock`
- Page-ingest lock: `/run/libyaintel/page-ingest.lock`
- If a lock is busy, the service logs:
  - `LOCK_SKIP job=summarize reason=busy`
  - `LOCK_SKIP job=page_ingest reason=busy`
  and exits 0 (no failed unit).
- Any other lock error logs `LOCK_ERROR ...` and exits non-zero.

## Lock contention test
Terminal A (hold lock):
- `sudo -u akram flock /run/libyaintel/summarize.lock -c "sleep 20" &`
- `sudo -u akram flock /run/libyaintel/page-ingest.lock -c "sleep 20" &`

Terminal B (start service while locked):
- `sudo systemctl start libyaintel-summarize.service`
- `sudo systemctl start libyaintel-page-ingest.service`
- `journalctl -u libyaintel-summarize.service -n 5 --no-pager`
- `journalctl -u libyaintel-page-ingest.service -n 5 --no-pager`

Expected: `LOCK_SKIP ...` in logs and exit success.

## Health checks
- `systemctl list-timers --all | grep libyaintel`
- `systemctl --failed`
- `journalctl -u libyaintel-summarize.service -n 50 --no-pager`
- `journalctl -u libyaintel-page-ingest.service -n 50 --no-pager`
- `journalctl -u libyaintel-alerts.service -n 50 --no-pager`
- Alerts telemetry:
  - `ALERTS_HEALTH ok=1 checked=... queued=... pending=... pending_due=... next_due_in_sec=... queued_at_estimated=... giveup=... oldest_age_sec=... send_ms=... db_ms=... other_ms=... total_ms=...`
  - `ALERTS_BACKLOG_WARN pending=... pending_due=... oldest_age_sec=... next_due_in_sec=... queued_at_estimated=... warn_count=... warn_age_sec=...`
  - `ALERTS_GIVEUP_WARN giveup=...`
  - `ALERTS_DEDUPE_SKIP user_id=... channel=... dedupe_key=... reason=recent_duplicate`
  - `ALERTS_GROUPED groups=... items=...`
  - `ALERTS_PRIORITY_COUNTS p0=... p1=... p2=...`

Alerts backlog definitions
- Retryable backlog (actionable):
  - `status IN ('PENDING','FAILED') AND attempt_count < ALERTS_MAX_ATTEMPTS`
- Dead letter backlog (permanent failures):
  - `status = 'FAILED' AND attempt_count >= ALERTS_MAX_ATTEMPTS`
- `oldest_age_sec` uses `queued_at` (enqueue timestamp).
- `queued_at_is_estimated=true` marks legacy/backfilled rows and is excluded from `oldest_age_sec`.
- Worker guard: the alerts worker refuses to run if any `queued_at` values are NULL.
- `pending_due` is runnable now: `next_attempt_at IS NULL OR next_attempt_at <= now()`.
- `next_due_in_sec` is the time until the next runnable item (0 when something is due).
- `queued_at_estimated` counts legacy rows (backfilled).
- `ALERTS_BACKLOG_WARN` triggers when `pending_due >= ALERTS_BACKLOG_WARN_COUNT` OR `oldest_age_sec >= ALERTS_BACKLOG_WARN_AGE_SEC`.

Alerts dedupe + priority
- Dedupe scope: `(user_id, channel, dedupe_key)` within `ALERTS_DEDUPE_WINDOW_SEC`.
- Dedupe key: normalized URL if present; else `source_name|normalized_title`.
- P0/P1/P2: P0 when saved search `query` is set; P1 when `category` is in `ALERTS_PRIORITY_CATEGORIES`; else P2.
- Dedupe key preview log (first few per run): `ALERTS_DEDUPE_KEY_PREVIEW article_id=... dedupe_key=...`
- Enable preview logging: `ALERTS_DEDUPE_DEBUG=1`
- Dedupe batch log: `ALERTS_DEDUPE_KEYS count=...`
- Dedupe check SQL:
  - `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/recent_dedupe_skips.sql`
- Dedupe smoke query:
  - `psql "$DATABASE_URL" -c "SELECT user_id, channel, dedupe_key, COUNT(*) AS n, MIN(created_at) AS first_seen, MAX(created_at) AS last_seen FROM alert_deliveries WHERE created_at > NOW() - INTERVAL '24 hours' GROUP BY 1,2,3 HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 50;"`
- Prefs log (per user): `ALERTS_PREFS user_id=... source=db|env dedupe_window=... immediate=... digest=... categories=...`

User alert prefs
- Table: `user_alert_prefs`
- Semantics:
  - `priority_categories = NULL` means "inherit env defaults".
  - `priority_categories = {}` (empty array) means "no P1 categories".
- Count rows:
  - `psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM user_alert_prefs;"`
- Show prefs for user:
  - `psql "$DATABASE_URL" -c "SELECT * FROM user_alert_prefs WHERE user_id = '<uuid>';"`
- Users with no prefs:
  - `psql "$DATABASE_URL" -c "SELECT DISTINCT d.user_id FROM alert_deliveries d LEFT JOIN user_alert_prefs p ON p.user_id = d.user_id WHERE p.user_id IS NULL LIMIT 50;"`
- Upsert helper:
  - `/opt/libyaintel/.venv/bin/python /opt/libyaintel/scripts/set_user_prefs.py --user-id <uuid> --dedupe-window-sec 7200 --immediate-priorities P0 --digest-priorities P1,P2 --priority-categories cbl,fx --digest-schedule daily --channels-enabled email`
  - Clear categories override:
    - `/opt/libyaintel/.venv/bin/python /opt/libyaintel/scripts/set_user_prefs.py --user-id <uuid> --clear-priority-categories`
  - Dry run:
    - `/opt/libyaintel/.venv/bin/python /opt/libyaintel/scripts/set_user_prefs.py --user-id <uuid> --dry-run`
  - Allowlist (optional):
    - `PREFS_ALLOWED_DB_HOSTS=localhost,127.0.0.1,db.internal`
  - Force override:
    - `/opt/libyaintel/.venv/bin/python /opt/libyaintel/scripts/set_user_prefs.py --user-id <uuid> --force`

Alerts schema bootstrap order (commands)
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/alerts_delivery_migration.sql`
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/migrations/20260201_alert_deliveries_dedupe.sql`
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/migrations/20260201_user_alert_prefs.sql`
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/migrations/20260201_alert_clicks.sql`
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/verify_alerts_schema.sql`
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/verify_user_prefs.sql`
- `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/verify_alert_clicks.sql`
- `sudo systemctl restart libyaintel-alerts.service`
- `journalctl -u libyaintel-alerts.service -n 50 --no-pager | grep ALERTS_HEALTH`

## Healthcheck
- `libyaintel-healthcheck.service` + `libyaintel-healthcheck.timer` (every 10 minutes, clock-aligned)
- Script: `/usr/local/bin/libyaintel_healthcheck.sh`
- Checks timer freshness:
  - summarize <= 10 min
  - page-ingest <= 20 min
  - daily-metrics <= 26 hours
- Logs:
  - `HEALTH_OK ...` on success
  - `HEALTH_FAIL ...` on failure (exit 1)

Quick run:
- `sudo systemctl start libyaintel-healthcheck.service`
- `journalctl -u libyaintel-healthcheck.service -n 20 --no-pager`

## DB size check
- `libyaintel-db-size-check.service` + `libyaintel-db-size-check.timer` (weekly, Sun 03:20)
- Script: `/usr/local/bin/libyaintel-db-size-check`
- SQL: `/opt/libyaintel/db/size_check.sql`
- Output: single line `MAINT_SIZE_OK top=...`

Quick run:
- `sudo systemctl start libyaintel-db-size-check.service`
- `journalctl -u libyaintel-db-size-check.service -n 5 --no-pager`

DB size check output format
- Example: `MAINT_SIZE_OK db_total=16 MB top=articles:2808 kB,feed_items:2792 kB,...`


## Alerts worker
- Unit: `libyaintel-alerts.service`
- Uses advisory lock + cursor table + retry with backoff.
- Hardened unit expects:
  - app path: `/opt/libyaintel`
  - service user: `libyaintel` (non-login)
  - writable state: `/var/lib/libyaintel`
- Env overrides:
  - `ALERTS_POLL_INTERVAL_SEC` (default 300, loop interval for long-running worker)
  - `ALERTS_MAX_PER_USER` (default 25)
  - `ALERTS_MAX_ITEMS_PER_EMAIL` (default 50)
  - `ALERTS_MAX_ATTEMPTS` (default 5)
  - `ALERTS_RETRY_BASE_SEC` (default 60)
  - `ALERTS_RETRY_MAX_SEC` (default 3600)
  - `ALERTS_CURSOR_OVERLAP_SEC` (default 86400)
  - `ALERTS_ADVISORY_LOCK_KEY` (default 743829113)
  - `ALERTS_BACKLOG_WARN_COUNT` (default 200)
  - `ALERTS_BACKLOG_WARN_AGE_SEC` (default 3600)
  - `ALERTS_DEDUPE_WINDOW_SEC` (default 21600)
  - `ALERTS_PRIORITY_CATEGORIES` (comma-separated, optional)
- Failure visibility: logs `ALERTS_DELIVERY_GIVEUP ...` when attempts reach `ALERTS_MAX_ATTEMPTS`.
- Startup version log: `ALERTS_START version=...` (reads `/opt/libyaintel/VERSION`).
- Heartbeat file: `/var/lib/libyaintel/alerts_last_ok.txt` (updated on each successful loop).
- Admin notifications (optional):
  - `ALERTS_ADMIN_TELEGRAM_BOT_TOKEN`
  - `ALERTS_ADMIN_TELEGRAM_CHAT_ID`
  - `ALERTS_ADMIN_EMAILS` (comma-separated)
  - `ALERTS_ADMIN_EMAIL_SUBJECT_PREFIX` (optional)
  - `ALERTS_ADMIN_NOTIFY_COOLDOWN_SEC` (default 3600)
  - `ALERTS_ADMIN_NOTIFY_GIVEUP_COOLDOWN_SEC` (default 3600)
- `ALERTS_ADMIN_NOTIFY_BACKLOG_COOLDOWN_SEC` (default 1800)
- `ALERTS_STALE_WARN_SEC` (default 900)
- `ALERTS_STALE_NOTIFY_COOLDOWN_SEC` (default 3600)

Admin notify test (safe)
- Add env overrides (temporary):
  - `ALERTS_MAX_ATTEMPTS=1`
  - `ALERTS_ADMIN_NOTIFY_COOLDOWN_SEC=0`
- Restart worker:
  - `sudo systemctl restart libyaintel-alerts.service`
- Watch notify results:
  - `journalctl -u libyaintel-alerts.service | grep ALERTS_ADMIN_NOTIFY`
- Or trigger a direct admin notify without affecting deliveries:
  - `/opt/libyaintel/.venv/bin/python /opt/libyaintel/scripts/admin_notify_test.py`
  - Optional: `--cooldown 0` or `--dry-run`

Admin notify error codes (err=)
- `not_configured` (channel not set up)
- `status_<code>` (Telegram HTTP status)
- `send_failed` (email send failed for one or more recipients)
- `exception` (unexpected exception in notifier)

Deploy (single script)
- Script: `/opt/libyaintel/scripts/deploy.sh [--skip-venv] [--skip-migrate] [--restart-all] [--cleanup-estimated] [--keep-old-venv] [--rollback-venv] <git-ref>`
- Notes:
  - Uses a deploy lock: `/var/lock/libyaintel-deploy.lock`.
  - Venv is rebuilt atomically using `.venv.new` -> `.venv`.
  - VERSION includes short commit hash and optional tag (`ref=...`).
- Writes `/opt/libyaintel/VERSION` (git commit hash).
- Runs migrations + sanity check, rebuilds venv, restarts alerts worker.

Deploy example (commands)
- `sudo /opt/libyaintel/scripts/deploy.sh <tag-or-commit>`
- `sudo /opt/libyaintel/scripts/deploy.sh --skip-venv --restart-all <tag-or-commit>`
- `sudo /opt/libyaintel/scripts/deploy.sh --cleanup-estimated <tag-or-commit>`
- Rollback venv (fast):
  - `cd /opt/libyaintel`
  - `sudo /opt/libyaintel/scripts/deploy.sh --rollback-venv`
  - Optional: `--restart-all`

Alerts watchdog (staleness)
- Units: `libyaintel-alerts-watchdog.service` + `libyaintel-alerts-watchdog.timer` (every 5 minutes)
- Script: `/opt/libyaintel/scripts/alerts_watchdog.py`
- Logs:
  - `ALERTS_STALE_OK age_sec=... warn_sec=...`
  - `ALERTS_STALE_WARN age_sec=... warn_sec=...`
  - `ALERTS_WATCHDOG_NO_HEARTBEAT missing_age_sec=... warn_sec=...`
- Manual check:
  - `/opt/libyaintel/.venv/bin/python /opt/libyaintel/scripts/alerts_watchdog.py --check`
- Install:
  - `sudo cp /opt/libyaintel/scripts/systemd/libyaintel-alerts-watchdog.service /etc/systemd/system/`
  - `sudo cp /opt/libyaintel/scripts/systemd/libyaintel-alerts-watchdog.timer /etc/systemd/system/`
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable --now libyaintel-alerts-watchdog.timer`

Backlog warn admin notify
- When `ALERTS_BACKLOG_WARN` fires, the worker also sends an admin notify (rate-limited).
- Backlog notify cooldown: `ALERTS_ADMIN_NOTIFY_BACKLOG_COOLDOWN_SEC`.
- Giveup notify cooldown: `ALERTS_ADMIN_NOTIFY_GIVEUP_COOLDOWN_SEC`.

## Post-migration watchlist (likely next problems)
1) No health signal
- Risk: you only "know it's alive" by checking journald.
- Fix: add a healthcheck endpoint or at least a heartbeat metric (last_success timestamp) and an external check.

2) Missed real failures
- Risk: fail-loud logs are still silent if nobody reads them.
- Fix: add a secondary alert channel (Telegram/email to admin) on `ALERTS_DELIVERY_GIVEUP`.

3) Falling behind without noticing
- Risk: queue depth grows while the service stays "running".
- Fix: log and/or expose `queue_depth`, `oldest_age`, `sent_rate` each cycle.

4) Deploy drift
- Risk: rsync to `/opt` will eventually get messy.
- Fix: one clean deploy method (git checkout + tagged release, or packaged build) and a single `deploy` script.

5) Dependency reproducibility
- Risk: "works on my venv" problems.
- Fix: pin dependencies (lockfile) and make `pip install` deterministic.

6) Systemd hardening blocks future changes
- Risk: permissions failures when the worker needs new write paths.
- Fix: explicitly define minimal writable paths and add a short troubleshooting section in OPS.

Hardened unit migration checklist
- `sudo useradd --system --no-create-home --shell /usr/sbin/nologin libyaintel`
- `sudo mkdir -p /opt/libyaintel /var/lib/libyaintel`
- `sudo rsync -a --delete /home/akram/libyaintel/ /opt/libyaintel/`
- `sudo chown -R libyaintel:libyaintel /opt/libyaintel /var/lib/libyaintel`
- `sudo cp /opt/libyaintel/scripts/systemd/libyaintel-alerts.service /etc/systemd/system/libyaintel-alerts.service`
- `sudo systemctl daemon-reload`
- `sudo systemctl enable --now libyaintel-alerts.service`

Rollback (alerts worker)
- `sudo systemctl stop libyaintel-alerts.service`
- `sudo cp /home/akram/libyaintel/scripts/systemd/libyaintel-alerts.service /etc/systemd/system/libyaintel-alerts.service`
- `sudo systemctl daemon-reload`
- `sudo systemctl disable --now libyaintel-alerts.service`
- `sudo systemctl unmask libyaintel-alerts.timer || true`
- `sudo systemctl enable --now libyaintel-alerts.timer`

Smoke test (alerts worker)
- `systemctl status libyaintel-alerts.service --no-pager`
- `journalctl -u libyaintel-alerts.service -n 50 --no-pager`

Schema/migration
- Files:
  - `/home/akram/libyaintel/alerts_delivery_migration.sql`
  - `/home/akram/libyaintel/migrations/20260201_alert_deliveries_dedupe.sql`
  - `/home/akram/libyaintel/migrations/20260201_user_alert_prefs.sql`
  - `/home/akram/libyaintel/migrations/20260201_alert_clicks.sql`
- Run in Supabase SQL Editor (order above).
- Includes `created_at`, `queued_at`, `queued_at_is_estimated`, `dedupe_key`, `dedupe_group`, `normalized_url`, `priority` on `alert_deliveries` and indexes for dedupe/backlog.
- Sanity check:
  - File: `/home/akram/libyaintel/scripts/verify_alerts_schema.sql`
  - Run (psql):
    - `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/verify_alerts_schema.sql`
- Prefs sanity check:
  - File: `/home/akram/libyaintel/scripts/verify_user_prefs.sql`
  - Run (psql):
    - `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/verify_user_prefs.sql`
- Clicks sanity check:
  - File: `/home/akram/libyaintel/scripts/verify_alert_clicks.sql`
  - Run (psql):
    - `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/verify_alert_clicks.sql`
- Cleanup (optional):
  - File: `/home/akram/libyaintel/scripts/cleanup_queued_at_estimated.sql`
  - Run (psql):
    - `psql "$DATABASE_URL" -f /home/akram/libyaintel/scripts/cleanup_queued_at_estimated.sql`

GDELT ingest
- One-shot run:
  - `/home/akram/libyaintel/backend/.venv/bin/python -m runner.jobs.gdelt_ingest`
- Enable timer:
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable --now libyaintel-gdelt-ingest.timer`
- Logs:
  - `journalctl -u libyaintel-gdelt-ingest.service -n 100 --no-pager`

Procurement discovery
- Config:
  - `/home/akram/libyaintel/runner/ingest/procurement_sources.json`
- One-shot run:
  - `/home/akram/libyaintel/backend/.venv/bin/python -m runner.jobs.procurement_discover`
- Enable timer (every 6 hours):
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable --now libyaintel-procurement-discover.timer`
- Logs:
  - `journalctl -u libyaintel-procurement-discover.service -n 100 --no-pager`
