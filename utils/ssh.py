"""SSH connection utilities for Bouchet HPC with Duo MFA bridge."""
import contextlib
import io
import os
import re
import time
import threading
import uuid
import logging
import paramiko

logger = logging.getLogger(__name__)

BOUCHET_USER = os.getenv("BOUCHET_USER", "yhs5")

# One shared Bouchet SSH session per API process (avoids repeated Duo prompts).
# Access is serialized with a lock because SSHClient is not thread-safe.
_ssh_pool_conn = None
_ssh_pool_last_used = None
_ssh_pool_lock = threading.RLock()

# How long to wait for the browser to answer a Duo prompt.
MFA_RESPONSE_TIMEOUT_SEC = int(os.getenv("MFA_RESPONSE_TIMEOUT_SEC", "150"))
SSH_BANNER_TIMEOUT_SEC = int(os.getenv("SSH_BANNER_TIMEOUT_SEC", "60"))
SSH_AUTH_TIMEOUT_SEC = int(os.getenv("SSH_AUTH_TIMEOUT_SEC", "180"))


class MfaBridge:
    """Process-wide bridge between Paramiko keyboard-interactive and the web UI."""

    def __init__(self):
        self._lock = threading.Lock()
        self._response_event = threading.Event()
        self._generation = 0
        self._response = None
        self._reset_unlocked(status="idle")

    def _reset_unlocked(self, status="idle", error=None):
        self.status = status  # idle|connecting|mfa_required|waiting_approval|authenticated|failed
        self.auth_id = None
        self.title = ""
        self.instructions = ""
        self.prompt = ""
        self.options = []
        self.error = error
        self.updated_at = time.time()
        self._response = None

    def snapshot(self):
        with self._lock:
            return {
                "status": self.status,
                "mfa_required": self.status == "mfa_required",
                "auth_id": self.auth_id,
                "title": self.title,
                "instructions": self.instructions,
                "prompt": self.prompt,
                "options": list(self.options),
                "error": self.error,
                "updated_at": self.updated_at,
            }

    def begin_connecting(self):
        with self._lock:
            self._generation += 1
            self._reset_unlocked(status="connecting")
            self._response_event.clear()

    def mark_authenticated(self):
        with self._lock:
            self.status = "authenticated"
            self.error = None
            self.auth_id = None
            self.updated_at = time.time()

    def mark_failed(self, message):
        with self._lock:
            self.status = "failed"
            self.error = str(message)
            self.auth_id = None
            self.updated_at = time.time()
            self._response_event.set()

    def mark_idle(self):
        with self._lock:
            if self.status in ("authenticated", "failed"):
                self._reset_unlocked(status="idle")
                self._response_event.clear()

    def mark_waiting_approval(self, title="", instructions=""):
        with self._lock:
            self.status = "waiting_approval"
            if title:
                self.title = title
            if instructions:
                self.instructions = instructions
            self.updated_at = time.time()

    @staticmethod
    def _parse_options(instructions, prompt_text):
        text = "\n".join(
            part for part in (instructions or "", prompt_text or "") if part
        )
        options = []
        for match in re.finditer(
            r"^\s*(\d+)\.\s+(.+?)\s*$", text, flags=re.MULTILINE
        ):
            options.append({"value": match.group(1), "label": match.group(2).strip()})
        return options

    def request_response(self, title, instructions, prompt_list, timeout=None):
        """Block the SSH auth thread until the frontend submits a choice."""
        timeout = MFA_RESPONSE_TIMEOUT_SEC if timeout is None else timeout
        prompt_text = "\n".join(p[0] for p in (prompt_list or []) if p and p[0])
        options = self._parse_options(instructions, prompt_text)

        with self._lock:
            self._generation += 1
            generation = self._generation
            self.status = "mfa_required"
            self.auth_id = str(uuid.uuid4())
            self.title = title or "Duo two-factor authentication"
            self.instructions = instructions or ""
            self.prompt = prompt_text or "Passcode or option:"
            self.options = options
            self.error = None
            self.updated_at = time.time()
            self._response = None
            self._response_event.clear()
            auth_id = self.auth_id

        logger.info(
            "MFA challenge pending auth_id=%s options=%s",
            auth_id,
            [o["value"] for o in options],
        )

        if not self._response_event.wait(timeout):
            self.mark_failed("Duo MFA timed out waiting for browser response")
            raise TimeoutError(
                "Duo MFA timed out waiting for user response in the web UI"
            )

        with self._lock:
            if self.status == "failed":
                raise Exception(self.error or "Duo MFA failed")
            if generation != self._generation:
                raise Exception("Duo MFA challenge was superseded")
            response = self._response
            if response is None:
                raise Exception("Duo MFA response missing")
            # After the choice is sent, Duo Push / phone call may still be in progress.
            self.status = "waiting_approval"
            self.updated_at = time.time()
            return response

    def submit_response(self, response, auth_id=None):
        response = (response or "").strip()
        if not response:
            return False, "Response is required"

        with self._lock:
            if self.status != "mfa_required":
                return False, "No Duo MFA challenge is currently pending"
            if auth_id and self.auth_id and auth_id != self.auth_id:
                return False, "Stale MFA challenge (auth_id mismatch)"
            self._response = response
            self.updated_at = time.time()
            self._response_event.set()
            return True, None


mfa_bridge = MfaBridge()


def _parse_private_key(private_key_str):
    private_key_str = private_key_str.replace("\\n", "\n")
    try:
        return paramiko.Ed25519Key.from_private_key(io.StringIO(private_key_str))
    except Exception:
        try:
            return paramiko.RSAKey.from_private_key(io.StringIO(private_key_str))
        except Exception:
            try:
                return paramiko.ECDSAKey.from_private_key(io.StringIO(private_key_str))
            except Exception:
                raise Exception("Could not parse SSH private key. Unsupported format.")


def _load_private_key():
    """
    Load the Bouchet SSH key.

    Preference order:
      1) SSH_PRIVATE_KEY_PATH / SSH_PRIVATE_KEY_FILE (local Yale key; needed for Duo)
      2) SSH_PRIVATE_KEY env string (e.g. deploy key)
    """
    key_path = os.getenv("SSH_PRIVATE_KEY_PATH") or os.getenv("SSH_PRIVATE_KEY_FILE")
    if key_path:
        key_path = os.path.expanduser(key_path.strip().strip('"').strip("'"))
        if not os.path.isfile(key_path):
            raise Exception(f"SSH private key file not found: {key_path}")
        with open(key_path, "r", encoding="utf-8") as f:
            return _parse_private_key(f.read()), key_path

    private_key_str = os.getenv("SSH_PRIVATE_KEY")
    if not private_key_str:
        raise Exception(
            "Set SSH_PRIVATE_KEY_PATH (recommended for local Yale Duo) "
            "or SSH_PRIVATE_KEY"
        )
    return _parse_private_key(private_key_str), "SSH_PRIVATE_KEY"


def _duo_interactive_handler(title, instructions, prompt_list):
    """Paramiko keyboard-interactive callback bridged to the frontend."""
    # Some Duo configs auto-push with no prompts; surface waiting state and continue.
    if not prompt_list:
        mfa_bridge.mark_waiting_approval(
            title=title or "Duo two-factor authentication",
            instructions=instructions
            or "Approve the Duo Push notification on your phone.",
        )
        return []

    # Yale Duo often puts the full menu into the prompt string, with empty instructions.
    prompt_text = "\n".join(p[0] for p in prompt_list if p and p[0])
    combined_instructions = "\n\n".join(
        part.strip()
        for part in (instructions or "", prompt_text or "")
        if part and part.strip()
    )
    response = mfa_bridge.request_response(
        title or "Duo two-factor authentication",
        combined_instructions,
        prompt_list,
    )
    # Duo usually sends a single "Passcode or option:" prompt.
    return [response for _ in prompt_list]


def _auth_interactive_via_web_ui(self, username, handler=None, submethods=""):
    """Replacement for Transport.auth_interactive_dumb that uses the web MFA bridge."""
    # Paramiko 3.3 signature: (username, handler=None, submethods="")
    return self.auth_interactive(username, _duo_interactive_handler, submethods)


@contextlib.contextmanager
def _web_ui_duo_interactive():
    """
    SSHClient.connect() uses auth_interactive_dumb after publickey partial success.
    Swap that to our bridge so Duo "1 / 2 / passcode" can be answered in the web UI.
    """
    original = paramiko.Transport.auth_interactive_dumb
    paramiko.Transport.auth_interactive_dumb = _auth_interactive_via_web_ui
    try:
        yield
    finally:
        paramiko.Transport.auth_interactive_dumb = original


def get_ssh_connection():
    """Create a new SSH connection to Bouchet (may trigger Duo via the web UI)."""
    hostname = os.getenv("BOUCHET_HOST")
    username = os.getenv("BOUCHET_USER")

    if not hostname:
        raise Exception("BOUCHET_HOST environment variable not set")
    if not username:
        raise Exception("BOUCHET_USER environment variable not set")

    private_key, key_source = _load_private_key()
    logger.info(
        "SSH auth as %s@%s using key from %s (%s)",
        username,
        hostname,
        key_source,
        private_key.get_name(),
    )

    mfa_bridge.begin_connecting()
    mfa_bridge.mark_waiting_approval(
        title="Yale HPC Duo verification",
        instructions=(
            "SSH key accepted — complete Duo on the next prompt. "
            "Choose option 1/2 or enter a passcode, then approve on your phone if asked."
        ),
    )

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        with _web_ui_duo_interactive():
            ssh.connect(
                hostname=hostname,
                username=username,
                pkey=private_key,
                timeout=SSH_AUTH_TIMEOUT_SEC,
                auth_timeout=SSH_AUTH_TIMEOUT_SEC,
                banner_timeout=SSH_BANNER_TIMEOUT_SEC,
                allow_agent=False,
                look_for_keys=False,
            )
        transport = ssh.get_transport()
        if transport:
            transport.set_keepalive(30)
        mfa_bridge.mark_authenticated()
        logger.info("SSH connection to %s authenticated", hostname)
        return ssh
    except paramiko.AuthenticationException as e:
        msg = str(e).rstrip(".")
        lower = msg.lower()
        if "timeout" in lower:
            hint = (
                f"{msg}. Duo was not completed in time. "
                "Submit option 1/2 in the browser modal and approve the push on your phone."
            )
        elif key_source == "SSH_PRIVATE_KEY":
            hint = (
                f"{msg}. The SSH_PRIVATE_KEY deploy key is not authorized on Bouchet. "
                "For local use set SSH_PRIVATE_KEY_PATH=~/.ssh/id_rsa (your Yale HPC key)."
            )
        else:
            hint = (
                f"{msg}. SSH/Duo authentication failed using key {key_source}. "
                "Retry and complete Duo in the browser modal."
            )
        mfa_bridge.mark_failed(hint)
        try:
            ssh.close()
        except Exception:
            pass
        raise paramiko.AuthenticationException(hint)
    except Exception as e:
        mfa_bridge.mark_failed(str(e))
        try:
            ssh.close()
        except Exception:
            pass
        raise

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
