#!/usr/bin/env bash
# Start the proxy API with a single worker + threads (required for Duo MFA bridge).
# On Yale Spinup, bind 0.0.0.0 so the frontend VM/browser can reach the API.
set -euo pipefail
cd "$(dirname "$0")"

exec gunicorn \
  --workers 1 \
  --threads 4 \
  --bind "${BIND_HOST:-0.0.0.0}:${PORT:-8000}" \
  --timeout 300 \
  app:app
