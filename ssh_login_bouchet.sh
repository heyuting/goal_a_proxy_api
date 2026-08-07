#!/usr/bin/env bash
# Establish an OpenSSH ControlMaster session to Bouchet (interactive Duo once).
# The Flask API then reuses this socket via BatchMode and never loads your key.
set -euo pipefail

HOST_ALIAS="${SSH_HOST_ALIAS:-bouchet}"
FQDN="${BOUCHET_HOST:-bouchet.ycrc.yale.edu}"
USER_NAME="${BOUCHET_USER:-yhs5}"
CONTROL_PATH="${SSH_CONTROL_PATH:-$HOME/.ssh/cm-bouchet-%r@%h:%p}"
CONTROL_PERSIST="${SSH_CONTROL_PERSIST:-yes}"
IDENTITY_FILE="${SSH_IDENTITY_FILE:-$HOME/.ssh/id_ed25519}"

mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

# "bouchet" is an SSH config Host alias, not a DNS name.
# If ~/.ssh/config has no Host bouchet, fall back to the real FQDN.
has_alias=0
if [[ -f "$HOME/.ssh/config" ]] && grep -Eq '^[[:space:]]*Host[[:space:]]+([^#]*[[:space:]])?bouchet([[:space:]]|$)' "$HOME/.ssh/config"; then
  has_alias=1
fi

TARGET="${HOST_ALIAS}"
EXTRA_OPTS=()
if [[ "${has_alias}" -eq 0 ]]; then
  echo "Note: no 'Host bouchet' entry in ~/.ssh/config — using ${USER_NAME}@${FQDN}"
  echo "Recommended (so 'ssh bouchet' works):"
  echo "  cat ssh_config.example >> ~/.ssh/config && chmod 600 ~/.ssh/config"
  echo
  TARGET="${FQDN}"
  EXTRA_OPTS+=(
    -o "User=${USER_NAME}"
    -o "IdentitiesOnly=yes"
  )
  if [[ -f "${IDENTITY_FILE}" ]]; then
    EXTRA_OPTS+=(-o "IdentityFile=${IDENTITY_FILE}")
  elif [[ -f "$HOME/.ssh/id_rsa" ]]; then
    EXTRA_OPTS+=(-o "IdentityFile=$HOME/.ssh/id_rsa")
  fi
fi

echo "Opening OpenSSH ControlMaster to '${TARGET}'..."
echo "Complete Duo in this terminal. ControlPersist=${CONTROL_PERSIST} keeps the master after you exit."
echo "ControlPath: ${CONTROL_PATH}"
echo

exec ssh \
  -o ControlMaster=yes \
  -o "ControlPath=${CONTROL_PATH}" \
  -o "ControlPersist=${CONTROL_PERSIST}" \
  "${EXTRA_OPTS[@]}" \
  "${TARGET}"
