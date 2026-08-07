"""
SSH utilities for Bouchet HPC via the system OpenSSH client.

Python never loads or manages private keys. OpenSSH reads ~/.ssh/*,
verifies known_hosts, runs the SSH protocol, and handles Duo
keyboard-interactive auth.

API requests use BatchMode against an existing ControlMaster session.
Authenticate once on the API host (interactive Duo), then the web app
reuses that multiplexed connection:

    ./ssh_login_bouchet.sh
"""
from __future__ import annotations

import contextlib
import io
import logging
import os
import subprocess
import tempfile
import threading
import time

logger = logging.getLogger(__name__)


def _bouchet_host():
    """Prefer BOUCHET_*; fall back to legacy GRACE_* names."""
    return os.getenv("BOUCHET_HOST") or os.getenv("GRACE_HOST") or "bouchet.ycrc.yale.edu"


def _bouchet_user():
    """Prefer BOUCHET_*; fall back to legacy GRACE_* names."""
    return os.getenv("BOUCHET_USER") or os.getenv("GRACE_USER") or "yhs5"


BOUCHET_USER = _bouchet_user()

# SSH host alias from ~/.ssh/config (recommended: Host bouchet).
SSH_HOST_ALIAS = os.getenv("SSH_HOST_ALIAS", "bouchet")
SSH_CONTROL_PATH = os.path.expanduser(
    os.getenv("SSH_CONTROL_PATH", "~/.ssh/cm-bouchet-%r@%h:%p")
)
SSH_CONTROL_PERSIST = os.getenv("SSH_CONTROL_PERSIST", "8h")
SSH_CONNECT_TIMEOUT_SEC = int(os.getenv("SSH_CONNECT_TIMEOUT_SEC", "30"))

# Kept for env-check compatibility; OpenSSH owns the key, not this process.
MFA_RESPONSE_TIMEOUT_SEC = int(os.getenv("MFA_RESPONSE_TIMEOUT_SEC", "150"))
SSH_BANNER_TIMEOUT_SEC = int(os.getenv("SSH_BANNER_TIMEOUT_SEC", "60"))
SSH_AUTH_TIMEOUT_SEC = int(os.getenv("SSH_AUTH_TIMEOUT_SEC", "180"))

_ssh_pool_lock = threading.RLock()
_ssh_pool_client = None
_ssh_pool_last_used = None


def ssh_credentials_configured():
    """
    True if we can invoke OpenSSH against the configured host alias.

    Does not require SSH_PRIVATE_KEY* in the environment — the key stays in
    ~/.ssh (or ssh-agent) under OpenSSH's control.
    """
    return bool(SSH_HOST_ALIAS and _bouchet_user())


def require_ssh_credentials():
    if ssh_credentials_configured():
        return
    raise Exception(
        "Set SSH_HOST_ALIAS (default: bouchet) and BOUCHET_USER (or GRACE_USER). "
        "Configure ~/.ssh/config and run ./ssh_login_bouchet.sh once for Duo."
    )


def _control_args():
    return [
        "-o",
        "ControlMaster=auto",
        "-o",
        f"ControlPath={SSH_CONTROL_PATH}",
        "-o",
        f"ControlPersist={SSH_CONTROL_PERSIST}",
    ]


def _ssh_base_cmd(host_alias=None, *, batch_mode=True):
    """Build ssh argv. BatchMode avoids hanging on Duo when no master exists."""
    cmd = ["ssh", *_control_args(), "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT_SEC}"]
    if batch_mode:
        cmd.extend(["-o", "BatchMode=yes"])
    cmd.append(host_alias or SSH_HOST_ALIAS)
    return cmd


def control_master_alive():
    """Return True if the OpenSSH ControlMaster socket is usable."""
    try:
        result = subprocess.run(
            [
                "ssh",
                *_control_args(),
                "-O",
                "check",
                SSH_HOST_ALIAS,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except Exception as e:
        logger.debug("ControlMaster check failed: %s", e)
        return False


class MfaBridge:
    """
    Status surface for the frontend.

    Duo is handled by OpenSSH on the API host (interactive login + ControlMaster),
    not by submitting passcodes through this Flask process.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.error = None
        self.updated_at = time.time()

    def snapshot(self):
        alive = control_master_alive()
        with self._lock:
            if alive:
                status = "authenticated"
                instructions = (
                    "OpenSSH ControlMaster is active. "
                    "The API reuses that session; Python does not load your private key."
                )
            else:
                status = "mfa_required"
                instructions = (
                    "No OpenSSH ControlMaster session. On the API (Spinup) host run:\n"
                    "  ./ssh_login_bouchet.sh\n"
                    "Complete Duo in that terminal once. Then retry the job from the web UI."
                )
            return {
                "status": status,
                "mfa_required": not alive,
                "auth_id": None,
                "title": "Yale HPC OpenSSH session",
                "instructions": instructions,
                "prompt": "",
                "options": [],
                "error": self.error,
                "updated_at": self.updated_at,
                "backend": "openssh",
                "host_alias": SSH_HOST_ALIAS,
                "control_path": SSH_CONTROL_PATH,
            }

    def begin_connecting(self):
        with self._lock:
            self.error = None
            self.updated_at = time.time()

    def mark_authenticated(self):
        with self._lock:
            self.error = None
            self.updated_at = time.time()

    def mark_failed(self, message):
        with self._lock:
            self.error = str(message)
            self.updated_at = time.time()

    def mark_idle(self):
        with self._lock:
            self.error = None
            self.updated_at = time.time()

    def mark_waiting_approval(self, title="", instructions=""):
        with self._lock:
            self.updated_at = time.time()

    def request_response(self, title, instructions, prompt_list, timeout=None):
        raise RuntimeError(
            "Browser Duo submission is not used with the OpenSSH backend. "
            "Run ./ssh_login_bouchet.sh on the API host instead."
        )

    def submit_response(self, response, auth_id=None):
        if control_master_alive():
            return True, None
        return (
            False,
            "Complete Duo on the API host with ./ssh_login_bouchet.sh "
            "(OpenSSH manages your key; the browser cannot inject the private key).",
        )


mfa_bridge = MfaBridge()


class _Channel:
    def __init__(self, returncode):
        self._returncode = returncode

    def recv_exit_status(self):
        return self._returncode

    def close(self):
        pass


class _Stream:
    def __init__(self, data: bytes, returncode: int = 0):
        self._buf = io.BytesIO(data)
        self.channel = _Channel(returncode)

    def read(self, size=-1):
        return self._buf.read(size)

    def close(self):
        self._buf.close()


class _NullStdin:
    def write(self, data):
        return len(data) if data is not None else 0

    def flush(self):
        pass

    def close(self):
        pass

    def channel(self):
        return None


class _SftpFile:
    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)

    def read(self, size=-1):
        return self._buf.read(size)

    def close(self):
        self._buf.close()


class OpenSFTP:
    """Minimal SFTP-like helper backed by `scp`."""

    def __init__(self, host_alias: str):
        self.host_alias = host_alias

    def open(self, remote_path, mode="rb"):
        if "r" not in mode:
            raise ValueError("OpenSFTP.open only supports read modes")
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_path = tmp.name
        try:
            cmd = [
                "scp",
                *_control_args(),
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={SSH_CONNECT_TIMEOUT_SEC}",
                f"{self.host_alias}:{remote_path}",
                local_path,
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=600)
            if result.returncode != 0:
                err = (result.stderr or b"").decode(errors="replace").strip()
                raise Exception(err or f"scp failed for {remote_path}")
            with open(local_path, "rb") as f:
                data = f.read()
            return _SftpFile(data)
        finally:
            try:
                os.unlink(local_path)
            except OSError:
                pass

    def close(self):
        pass


class OpenSSHClient:
    """
    Thin facade matching the Paramiko methods used by blueprints:
    exec_command, open_sftp, close.
    """

    def __init__(self, host_alias: str = None):
        self.host_alias = host_alias or SSH_HOST_ALIAS

    def exec_command(self, command, timeout=None):
        cmd = _ssh_base_cmd(self.host_alias, batch_mode=True) + [command]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=timeout if timeout is not None else None,
            )
        except subprocess.TimeoutExpired as e:
            raise TimeoutError(f"SSH command timed out: {command[:120]}") from e

        if result.returncode != 0 and not result.stdout and result.stderr:
            err = result.stderr.decode(errors="replace").strip()
            lower = err.lower()
            if (
                "permission denied" in lower
                or "authentication" in lower
                or "there are no available authentication methods" in lower
            ):
                hint = (
                    f"{err}. OpenSSH has no usable ControlMaster session. "
                    "On the API host run ./ssh_login_bouchet.sh and complete Duo, "
                    "then retry."
                )
                mfa_bridge.mark_failed(hint)
                raise Exception(hint)

        stdout = _Stream(result.stdout or b"", result.returncode)
        stderr = _Stream(result.stderr or b"", result.returncode)
        return _NullStdin(), stdout, stderr

    def open_sftp(self):
        return OpenSFTP(self.host_alias)

    def close(self):
        # Multiplexed OpenSSH sessions are owned by ssh ControlMaster, not us.
        pass

    def get_transport(self):
        return _FakeTransport(alive=control_master_alive())


class _FakeTransport:
    def __init__(self, alive=True):
        self._alive = alive

    def is_active(self):
        return self._alive

    def is_alive(self):
        return self._alive

    def set_keepalive(self, _seconds):
        pass


def get_ssh_connection():
    """
    Return an OpenSSH-backed client.

    Requires an existing ControlMaster (./ssh_login_bouchet.sh). Does not load
    private keys into Python.
    """
    require_ssh_credentials()
    mfa_bridge.begin_connecting()

    if not control_master_alive():
        hint = (
            "No OpenSSH ControlMaster for "
            f"{SSH_HOST_ALIAS}. Run ./ssh_login_bouchet.sh on the API host, "
            "complete Duo once, then retry. Python does not manage your private key."
        )
        mfa_bridge.mark_failed(hint)
        raise Exception(hint)

    # Cheap connectivity check
    client = OpenSSHClient()
    stdin, stdout, stderr = client.exec_command("true")
    code = stdout.channel.recv_exit_status()
    if code != 0:
        err = stderr.read().decode(errors="replace").strip()
        hint = err or "OpenSSH ControlMaster check command failed"
        mfa_bridge.mark_failed(hint)
        raise Exception(hint)

    mfa_bridge.mark_authenticated()
    logger.info(
        "Using OpenSSH ControlMaster session for %s (user=%s, key managed by OpenSSH)",
        SSH_HOST_ALIAS,
        _bouchet_user(),
    )
    return client


def _ensure_pooled_connection():
    global _ssh_pool_client, _ssh_pool_last_used
    current_time = time.time()
    if _ssh_pool_client is not None and control_master_alive():
        if _ssh_pool_last_used is None or (current_time - _ssh_pool_last_used) < 1800:
            return _ssh_pool_client
    _ssh_pool_client = get_ssh_connection()
    return _ssh_pool_client


def reset_ssh_connection_pool():
    """Drop the in-process client handle (does not kill OpenSSH ControlMaster)."""
    global _ssh_pool_client, _ssh_pool_last_used
    with _ssh_pool_lock:
        _ssh_pool_client = None
        _ssh_pool_last_used = None


@contextlib.contextmanager
def bouchet_ssh_session():
    """Reuse OpenSSH ControlMaster across requests."""
    global _ssh_pool_last_used
    with _ssh_pool_lock:
        ssh = _ensure_pooled_connection()
        try:
            yield ssh
        finally:
            _ssh_pool_last_used = time.time()


def get_ssh_connection_pooled():
    """Deprecated: prefer bouchet_ssh_session()."""
    with _ssh_pool_lock:
        return _ensure_pooled_connection()


def ssh_exec_read(ssh, cmd, timeout=None):
    """Run a remote command and return stdout as text."""
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    try:
        out = stdout.read().decode()
        stderr.read()
        stdout.channel.recv_exit_status()
        return out
    except Exception:
        try:
            stdout.close()
        except Exception:
            pass
        raise


def ssh_run(command, timeout=None):
    """Convenience: run a remote command via OpenSSH and return CompletedProcess-like data."""
    with bouchet_ssh_session() as ssh:
        stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout)
        code = stdout.channel.recv_exit_status()
        return code, stdout.read(), stderr.read()
