#!/usr/bin/env bash
set -euo pipefail

cd /opt/libyaintel

if [ -n "$(git status --porcelain)" ]; then
  echo "DEPLOY_ABORT dirty git working tree"
  git status --porcelain
  exit 1
fi

echo "DEPLOY_PULL"
git fetch origin
git reset --hard origin/main

echo "DEPLOY_VENV"
cd /opt/libyaintel/backend
if [ ! -x /opt/libyaintel/backend/.venv/bin/pip ]; then
  python3 -m venv /opt/libyaintel/backend/.venv
fi
/opt/libyaintel/backend/.venv/bin/pip install -r requirements.txt

echo "DEPLOY_MIGRATIONS"
if [ -f "/etc/libyaintel/libyaintel.env" ]; then
  set -a
  source /etc/libyaintel/libyaintel.env
  set +a
fi
for f in /opt/libyaintel/backend/migrations/*.sql; do
  echo "APPLY $(basename "$f")"
  psql "$DATABASE_URL" -f "$f"
done

echo "DEPLOY_SYSTEMD"
sudo systemctl daemon-reload
sudo systemctl enable --now libyaintel-extract-tenders.timer
sudo systemctl enable --now libyaintel-procurement-digest.timer
sudo systemctl restart libyaintel-extract-tenders.service || true
sudo systemctl restart libyaintel-procurement-digest.service || true

echo "DEPLOY_OK"
