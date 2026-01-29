"""SSH connection utilities for Grace HPC."""
import io
import os
import time
import threading
import paramiko

GRACE_USER = os.getenv("GRACE_USER", "yhs5")

# SSH connection pool to avoid repeated DUO authentication
_ssh_connection_pool = None
_ssh_connection_lock = None
_ssh_connection_last_used = None


def get_ssh_connection():
    """Create SSH connection to Grace server"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    hostname = os.getenv("GRACE_HOST")
    username = os.getenv("GRACE_USER")
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


def get_ssh_connection_pooled():
    """Get or reuse SSH connection to avoid repeated DUO authentication"""
    global _ssh_connection_pool, _ssh_connection_lock, _ssh_connection_last_used

    if _ssh_connection_lock is None:
        _ssh_connection_lock = threading.Lock()

    with _ssh_connection_lock:
        current_time = time.time()
        if _ssh_connection_pool is not None:
            connection_alive = False
            transport = _ssh_connection_pool.get_transport()
            if transport is not None:
                try:
                    connection_alive = transport.is_active()
                    if connection_alive:
                        connection_alive = (
                            transport.is_alive()
                            if hasattr(transport, "is_alive")
                            else transport.is_active()
                        )
                except Exception:
                    connection_alive = False

            if connection_alive:
                if (
                    _ssh_connection_last_used is None
                    or (current_time - _ssh_connection_last_used) < 1800
                ):
                    _ssh_connection_last_used = current_time
                    return _ssh_connection_pool
                else:
                    try:
                        _ssh_connection_pool.close()
                    except Exception:
                        pass
                    _ssh_connection_pool = None
            else:
                try:
                    _ssh_connection_pool.close()
                except Exception:
                    pass
                _ssh_connection_pool = None

        _ssh_connection_pool = get_ssh_connection()
        _ssh_connection_last_used = current_time
        return _ssh_connection_pool
