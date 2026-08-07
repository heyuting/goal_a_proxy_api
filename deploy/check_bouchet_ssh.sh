#!/usr/bin/env bash
# Check OpenSSH ControlMaster to Bouchet; exit 0 if up, 1 if down.
# Optional: set NOTIFY_EMAIL to mail on failure (requires a local mailer).
#
# Cron example (every 10 minutes):
#   */10 * * * * /home/yhs5/webapp/goal_a_proxy_api/deploy/check_bouchet_ssh.sh
set -euo pipefail

HOST_ALIAS="${SSH_HOST_ALIAS:-bouchet}"
NOTIFY_EMAIL="${NOTIFY_EMAIL:-}"

if ssh -O check "${HOST_ALIAS}" >/dev/null 2>&1; then
  exit 0
fi

MSG="GOAL-A: OpenSSH ControlMaster for '${HOST_ALIAS}' is down on $(hostname) at $(date -Is). Run ./ssh_login_bouchet.sh on the API host."
echo "${MSG}" >&2

if [[ -n "${NOTIFY_EMAIL}" ]] && command -v mail >/dev/null 2>&1; then
  echo "${MSG}" | mail -s "GOAL-A HPC session down on $(hostname)" "${NOTIFY_EMAIL}" || true
fi

exit 1
