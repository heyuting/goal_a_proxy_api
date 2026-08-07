#!/usr/bin/env bash
# Establish an OpenSSH ControlMaster session to Bouchet (interactive Duo once).
# The Flask API then reuses this socket via BatchMode and never loads your key.
set -euo pipefail

HOST_ALIAS="${SSH_HOST_ALIAS:-bouchet}"
CONTROL_PATH="${SSH_CONTROL_PATH:-$HOME/.ssh/cm-bouchet-%r@%h:%p}"
CONTROL_PERSIST="${SSH_CONTROL_PERSIST:-8h}"

mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

echo "Opening OpenSSH ControlMaster to '${HOST_ALIAS}'..."
echo "Complete Duo in this terminal. Leave this running until ControlPersist takes over."
echo "ControlPath: ${CONTROL_PATH}"
echo

# Expand %tokens by asking ssh; for display we keep the template in env as-is.
exec ssh \
  -o ControlMaster=yes \
  -o "ControlPath=${CONTROL_PATH}" \
  -o "ControlPersist=${CONTROL_PERSIST}" \
  "${HOST_ALIAS}"
