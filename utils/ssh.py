"""SSH connection utilities for Bouchet HPC."""
import io
import os
import time
import threading
import paramiko

BOUCHET_USER = os.getenv("BOUCHET_USER", "yhs5")

# Per-thread pooled SSH connections (SSHClient is not safe across threads).
_ssh_thread_local = threading.local()
_ssh_create_lock = threading.Lock()


def get_ssh_connection():
    """Create SSH connection to Bouchet server"""
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


def reset_ssh_connection_pool():
    """Drop the current thread's pooled SSH connection (e.g. after channel errors)."""
    conn = getattr(_ssh_thread_local, "ssh", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    _ssh_thread_local.ssh = None
    _ssh_thread_local.last_used = None


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


def get_ssh_connection_pooled():
    """Get or reuse a per-thread SSH connection to avoid repeated DUO auth."""
    current_time = time.time()
    conn = getattr(_ssh_thread_local, "ssh", None)
    last_used = getattr(_ssh_thread_local, "last_used", None)

    if conn is not None and _transport_alive(conn):
        if last_used is None or (current_time - last_used) < 1800:
            _ssh_thread_local.last_used = current_time
            return conn
        try:
            conn.close()
        except Exception:
            pass

    with _ssh_create_lock:
        conn = get_ssh_connection()
        _ssh_thread_local.ssh = conn
        _ssh_thread_local.last_used = current_time
        return conn
