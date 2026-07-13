"""SSH connection utilities for Bouchet HPC."""
import contextlib
import io
import os
import time
import threading
import paramiko

BOUCHET_USER = os.getenv("BOUCHET_USER", "yhs5")

# One shared Bouchet SSH session per API process (avoids repeated Duo prompts).
# Access is serialized with a lock because SSHClient is not thread-safe.
_ssh_pool_conn = None
_ssh_pool_last_used = None
_ssh_pool_lock = threading.RLock()


def get_ssh_connection():
    """Create a new SSH connection to Bouchet (triggers Duo)."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    hostname = os.getenv("BOUCHET_HOST")
    username = os.getenv("BOUCHET_USER")
    private_key_str = os.getenv("SSH_PRIVATE_KEY")

    if not private_key_str:
        raise Exception("SSH_PRIVATE_KEY environment variable not set")

    # Clean up the key string - fix line breaks
    private_key_str = private_key_str.replace("\\n", "\n")

    # Try different key types (OpenSSH format is usually Ed25519)
    try:
        private_key = paramiko.Ed25519Key.from_private_key(io.StringIO(private_key_str))
    except Exception:
        try:
            private_key = paramiko.RSAKey.from_private_key(io.StringIO(private_key_str))
        except Exception:
            try:
                private_key = paramiko.ECDSAKey.from_private_key(
                    io.StringIO(private_key_str)
                )
            except Exception:
                raise Exception("Could not parse SSH private key. Unsupported format.")

    ssh.connect(
        hostname=hostname,
        username=username,
        pkey=private_key,
        timeout=180,
        auth_timeout=180,
    )
    transport = ssh.get_transport()
    if transport:
        transport.set_keepalive(30)
    return ssh


def _transport_alive(ssh):
    if ssh is None:
        return False
    transport = ssh.get_transport()
    if transport is None:
        return False
    try:
        if not transport.is_active():
            return False
        if hasattr(transport, "is_alive"):
            return transport.is_alive()
        return True
    except Exception:
        return False


def _ensure_pooled_connection():
    """Return the shared connection, creating it if needed. Caller must hold _ssh_pool_lock."""
    global _ssh_pool_conn, _ssh_pool_last_used
    current_time = time.time()
    if _ssh_pool_conn is not None and _transport_alive(_ssh_pool_conn):
        if _ssh_pool_last_used is None or (current_time - _ssh_pool_last_used) < 1800:
            return _ssh_pool_conn
        try:
            _ssh_pool_conn.close()
        except Exception:
            pass
        _ssh_pool_conn = None
    elif _ssh_pool_conn is not None:
        try:
            _ssh_pool_conn.close()
        except Exception:
            pass
        _ssh_pool_conn = None

    _ssh_pool_conn = get_ssh_connection()
    return _ssh_pool_conn


def reset_ssh_connection_pool():
    """Drop the shared SSH connection (e.g. after channel errors)."""
    global _ssh_pool_conn, _ssh_pool_last_used
    with _ssh_pool_lock:
        if _ssh_pool_conn is not None:
            try:
                _ssh_pool_conn.close()
            except Exception:
                pass
        _ssh_pool_conn = None
        _ssh_pool_last_used = None


@contextlib.contextmanager
def bouchet_ssh_session():
    """Reuse one Bouchet SSH login across requests; safe for concurrent Flask threads."""
    global _ssh_pool_last_used
    with _ssh_pool_lock:
        ssh = _ensure_pooled_connection()
        try:
            yield ssh
        finally:
            _ssh_pool_last_used = time.time()


def get_ssh_connection_pooled():
    """Deprecated: use bouchet_ssh_session() so the pool lock is held for the whole operation."""
    with _ssh_pool_lock:
        return _ensure_pooled_connection()


def ssh_exec_read(ssh, cmd, timeout=None):
    """Run a remote command and fully drain the channel before returning."""
    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    stdin, stdout, stderr = ssh.exec_command(cmd, **kwargs)
    try:
        out = stdout.read().decode()
        stderr.read()
        stdout.channel.recv_exit_status()
        return out
    except Exception:
        try:
            stdout.channel.close()
        except Exception:
            pass
        raise
