#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

: "${F1_RADAR_ADMIN_TOKEN:?Set F1_RADAR_ADMIN_TOKEN in .env}"

exec /opt/anaconda3/bin/python3 -m uvicorn app:app --host 127.0.0.1 --port "${PORT:-8000}"
