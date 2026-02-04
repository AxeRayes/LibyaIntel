#!/usr/bin/env bash
set -euo pipefail
trap 'echo "deploy failed on line $LINENO" >&2' ERR

SKIP_VENV=0
SKIP_MIGRATE=0
RESTART_ALL=0
CLEANUP_ESTIMATED=0
KEEP_OLD_VENV=0
ROLLBACK_VENV=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-venv) SKIP_VENV=1; shift ;;
    --skip-migrate) SKIP_MIGRATE=1; shift ;;
    --restart-all) RESTART_ALL=1; shift ;;
    --cleanup-estimated) CLEANUP_ESTIMATED=1; shift ;;
    --keep-old-venv) KEEP_OLD_VENV=1; shift ;;
    --rollback-venv) ROLLBACK_VENV=1; shift ;;
    -*)
      echo "unknown option: $1"
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

LOCK_FILE="/var/lock/libyaintel-deploy.lock"
exec 9>"$LOCK_FILE"
flock -n 9 || { echo "deploy already running"; exit 1; }

rollback_venv() {
  echo "Rolling back venv..."
  if [[ ! -d ".venv.old" ]]; then
    echo "ERROR: .venv.old not found. Cannot rollback." >&2
    exit 1
  fi
  systemctl stop libyaintel-alerts.service
  rm -rf .venv.bad || true
  mv .venv .venv.bad 2>/dev/null || true
  mv .venv.old .venv
  systemctl start libyaintel-alerts.service
  sleep 2
  rm -rf .venv.bad || true
  echo "Rollback complete."
  journalctl -u libyaintel-alerts.service -n 5 --no-pager | grep ALERTS_START || true
}

if [[ "$ROLLBACK_VENV" -eq 1 ]]; then
  cd "${DEPLOY_DIR:-/opt/libyaintel}"
  rollback_venv
  if [[ "$RESTART_ALL" -eq 1 ]]; then
    systemctl restart libyaintel-summarize.timer || true
    systemctl restart libyaintel-page-ingest.timer || true
    systemctl restart libyaintel-healthcheck.timer || true
    systemctl restart libyaintel-db-size-check.timer || true
    systemctl restart libyaintel-alerts-watchdog.timer || true
    systemctl mask --now libyaintel-alerts.timer || true
    journalctl -u libyaintel-alerts.service -n 5 --no-pager | grep ALERTS_START || true
  fi
  exit 0
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 [--skip-venv] [--skip-migrate] [--restart-all] [--cleanup-estimated] [--keep-old-venv] [--rollback-venv] <git-ref>"
  exit 1
fi

REF="$1"
DEPLOY_DIR="${DEPLOY_DIR:-/opt/libyaintel}"
cd "$DEPLOY_DIR"

git fetch --tags --prune
git checkout -f "$REF"
git reset --hard "$REF"

GIT_HASH="$(git rev-parse --short HEAD)"
GIT_TAG="$(git describe --tags --exact-match 2>/dev/null || true)"
if [[ -n "$GIT_TAG" ]]; then
  printf "%s\nref=%s\n" "$GIT_HASH" "$GIT_TAG" > VERSION
else
  printf "%s\n" "$GIT_HASH" > VERSION
fi

if [[ "$SKIP_VENV" -eq 0 ]]; then
  if command -v python3 >/dev/null 2>&1; then
    python3 -m venv .venv.new
    .venv.new/bin/pip install --upgrade pip
    if [[ -f requirements.txt ]]; then
      .venv.new/bin/pip install -r requirements.txt
    elif [[ -f backend/requirements.txt ]]; then
      .venv.new/bin/pip install -r backend/requirements.txt
    else
      echo "requirements.txt not found; skipping pip install"
    fi
    if .venv.new/bin/python -c "import playwright" >/dev/null 2>&1; then
      .venv.new/bin/python -m playwright install --with-deps chromium
    fi
    if [[ "$KEEP_OLD_VENV" -eq 0 ]]; then
      rm -rf .venv.old || true
    fi
    if [[ -d .venv ]]; then
      mv .venv .venv.old
    fi
    mv .venv.new .venv
  else
    echo "python3 not found; skipping venv rebuild"
  fi
else
  echo "skip venv rebuild"
fi

if [[ "$CLEANUP_ESTIMATED" -eq 1 ]]; then
  SKIP_MIGRATE=0
  if [[ -z "${DATABASE_URL:-}" ]]; then
    echo "DATABASE_URL is required for --cleanup-estimated"
    exit 1
  fi
fi

if [[ "$SKIP_MIGRATE" -eq 0 ]]; then
  if [[ -n "${DATABASE_URL:-}" ]]; then
    psql "$DATABASE_URL" -f alerts_delivery_migration.sql
    psql "$DATABASE_URL" -f migrations/20260201_alert_deliveries_dedupe.sql
    psql "$DATABASE_URL" -f migrations/20260201_user_alert_prefs.sql
    psql "$DATABASE_URL" -f migrations/20260201_alert_clicks.sql
    psql "$DATABASE_URL" -f migrations/20260203_unsmil_quality_fields.sql
    psql "$DATABASE_URL" -f migrations/20260203_fetch_queue.sql
    psql "$DATABASE_URL" -f migrations/20260204_gdelt_source.sql
    psql "$DATABASE_URL" -f migrations/20260204_procurement_source.sql
    psql "$DATABASE_URL" -f migrations/20260204_tenders.sql
    if [[ "$CLEANUP_ESTIMATED" -eq 1 ]]; then
      psql "$DATABASE_URL" -f scripts/cleanup_queued_at_estimated.sql
    fi
    psql "$DATABASE_URL" -f scripts/verify_alerts_schema.sql
    psql "$DATABASE_URL" -f scripts/verify_user_prefs.sql
    psql "$DATABASE_URL" -f scripts/verify_alert_clicks.sql
  else
    echo "DATABASE_URL not set; skipping migrations"
  fi
else
  echo "skip migrations"
fi

if [[ -f systemd/libyaintel-gdelt-ingest.service ]]; then
  install -m 0644 systemd/libyaintel-gdelt-ingest.service /etc/systemd/system/
  install -m 0644 systemd/libyaintel-gdelt-ingest.timer /etc/systemd/system/
  if [[ -f systemd/libyaintel-gdelt-ingest-ar.service ]]; then
    install -m 0644 systemd/libyaintel-gdelt-ingest-ar.service /etc/systemd/system/
    install -m 0644 systemd/libyaintel-gdelt-ingest-ar.timer /etc/systemd/system/
  fi
  systemctl daemon-reload || true
  systemctl enable --now libyaintel-gdelt-ingest.timer || true
  systemctl enable --now libyaintel-gdelt-ingest-ar.timer || true
fi

if [[ -f systemd/libyaintel-procurement-discover.service ]]; then
  install -m 0644 systemd/libyaintel-procurement-discover.service /etc/systemd/system/
  install -m 0644 systemd/libyaintel-procurement-discover.timer /etc/systemd/system/
  systemctl daemon-reload || true
  systemctl enable --now libyaintel-procurement-discover.timer || true
fi

if [[ -f systemd/libyaintel-extract-tenders.service ]]; then
  install -m 0644 systemd/libyaintel-extract-tenders.service /etc/systemd/system/
  install -m 0644 systemd/libyaintel-extract-tenders.timer /etc/systemd/system/
  systemctl daemon-reload || true
  systemctl enable --now libyaintel-extract-tenders.timer || true
fi

if [[ -f systemd/libyaintel-procurement-digest.service ]]; then
  install -m 0644 systemd/libyaintel-procurement-digest.service /etc/systemd/system/
  install -m 0644 systemd/libyaintel-procurement-digest.timer /etc/systemd/system/
  systemctl daemon-reload || true
  systemctl enable --now libyaintel-procurement-digest.timer || true
fi

if [[ "$RESTART_ALL" -eq 1 ]]; then
  systemctl restart libyaintel-alerts.service
  systemctl restart libyaintel-summarize.timer || true
  systemctl restart libyaintel-page-ingest.timer || true
  systemctl restart libyaintel-healthcheck.timer || true
  systemctl restart libyaintel-db-size-check.timer || true
  systemctl mask --now libyaintel-alerts.timer || true
else
  systemctl restart libyaintel-alerts.service
fi

echo "deploy complete: $(cat VERSION)"
