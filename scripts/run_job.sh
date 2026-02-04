#!/usr/bin/env bash
set -euo pipefail

JOB="${1:-}"
if [[ -z "$JOB" ]]; then
  echo "Usage: run_job.sh <module.path>"
  exit 2
fi

# Adjust if your venv path differs:
VENV="/home/akram/libyaintel/backend/.venv"
PY="$VENV/bin/python3"

cd /home/akram/libyaintel

# Default env (override in unit files if needed)
export PYTHONUNBUFFERED=1
export TZ=UTC

# Run
exec "$PY" -m "$JOB"
