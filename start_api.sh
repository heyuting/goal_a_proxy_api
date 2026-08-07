#!/usr/bin/env bash
# Foreground helper for local debugging.
# On Spinup for always-on: use deploy/install_systemd_service.sh instead.
set -euo pipefail
cd "$(dirname "$0")"

exec gunicorn \
  --workers 1 \
  --threads 4 \
  --bind "${BIND_HOST:-0.0.0.0}:${PORT:-8000}" \
  --timeout 300 \
  app:app
