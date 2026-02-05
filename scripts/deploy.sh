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

echo "DEPLOY_WEB"
cd /opt/libyaintel/web
if [ -f package-lock.json ]; then
  npm ci
else
  npm install
fi
npm run build
sudo chown -R libyaintel:libyaintel /opt/libyaintel/web/.next /opt/libyaintel/web/node_modules 2>/dev/null || true

echo "DEPLOY_MIGRATIONS"
if [ -f "/etc/libyaintel/libyaintel.env" ]; then
  set -a
  source /etc/libyaintel/libyaintel.env
  set +a
fi
shopt -s nullglob
for f in /opt/libyaintel/migrations/*.sql; do
  echo "APPLY $(basename "$f")"
  psql "$DATABASE_URL" -f "$f"
done

echo "DEPLOY_SYSTEMD"
sudo install -m 0644 /opt/libyaintel/systemd/libyaintel-api.service /etc/systemd/system/libyaintel-api.service
sudo install -m 0644 /opt/libyaintel/systemd/libyaintel-market-quotes.service /etc/systemd/system/libyaintel-market-quotes.service
sudo install -m 0644 /opt/libyaintel/systemd/libyaintel-market-quotes.timer /etc/systemd/system/libyaintel-market-quotes.timer
sudo install -m 0644 /opt/libyaintel/systemd/libyaintel-web.service /etc/systemd/system/libyaintel-web.service
sudo systemctl daemon-reload
sudo systemctl enable --now libyaintel-api.service
sudo systemctl enable --now libyaintel-extract-tenders.timer
sudo systemctl enable --now libyaintel-market-quotes.timer
sudo systemctl enable --now libyaintel-procurement-digest.timer
sudo systemctl enable --now libyaintel-web.service
sudo systemctl restart libyaintel-api.service || true
sudo systemctl restart libyaintel-extract-tenders.service || true
sudo systemctl restart libyaintel-procurement-digest.service || true
sudo systemctl restart libyaintel-web.service || true

echo "DEPLOY_OK"
