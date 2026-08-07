#!/usr/bin/env bash
# Install / refresh the GOAL-A API systemd unit on Yale Spinup.
# Usage (on the API VM):
#   ./deploy/install_systemd_service.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UNIT_SRC="${ROOT}/deploy/goal-a-api.service"
UNIT_DST="/etc/systemd/system/goal-a-api.service"

if [[ ! -f "${UNIT_SRC}" ]]; then
  echo "Missing unit file: ${UNIT_SRC}" >&2
  exit 1
fi

echo "Installing ${UNIT_DST}"
sudo cp "${UNIT_SRC}" "${UNIT_DST}"
sudo systemctl daemon-reload
sudo systemctl enable goal-a-api.service
sudo systemctl restart goal-a-api.service
sudo systemctl --no-pager --full status goal-a-api.service || true

echo
echo "Useful commands:"
echo "  sudo systemctl status goal-a-api"
echo "  sudo journalctl -u goal-a-api -f"
echo "  sudo systemctl restart goal-a-api"
echo
echo "Bouchet SSH is separate — after reboot run:  ./ssh_login_bouchet.sh"
