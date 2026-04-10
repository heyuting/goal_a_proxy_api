"""
SCEPTER blueprint: baseline simulation (spinup), run model (restart_add_gbas), status, results, download.
Uses spinup + restart flow (no standalone pipeline).
"""

from flask import Blueprint, request, jsonify, make_response, current_app
import logging
import os
import json
import time
import re
import shlex
import threading

from utils.ssh import get_ssh_connection, get_ssh_connection_pooled, BOUCHET_USER

scepter_bp = Blueprint("scepter", __name__)
JOB_STATUS_CACHE = {}  # job_id -> { job_id, bouchet_job_id, job_folder, status, ... }
BATCH_JOB_CACHE = {}  # batch_id -> { batch_id, job_ids: [...], submitted_at }

_scepter_log = logging.getLogger(__name__)
_SCEPTER_REGISTRY_LOCK = threading.Lock()
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _scepter_batch_registry_path():
    p = os.getenv("SCEPTER_BATCH_REGISTRY")
    if p:
        return os.path.abspath(p)
    return os.path.join(_PROJECT_ROOT, "scepter_batch_registry.json")


def _load_scepter_batch_registry():
    """Restore batch metadata after API restart (BATCH_JOB_CACHE is in-memory only)."""
    path = _scepter_batch_registry_path()
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        with _SCEPTER_REGISTRY_LOCK:
            for bid, rec in data.items():
                if isinstance(rec, dict) and isinstance(bid, str):
                    BATCH_JOB_CACHE[bid] = rec
    except Exception as e:
        _scepter_log.warning("Could not load SCEPTER batch registry %s: %s", path, e)


def _persist_scepter_batch_record(batch_id, record):
    """Append/update one batch in the on-disk registry."""
    path = _scepter_batch_registry_path()
    slim = {
        "batch_id": record.get("batch_id") or batch_id,
        "job_ids": list(record.get("job_ids") or []),
        "submitted_at": record.get("submitted_at"),
        "job_type": record.get("job_type") or "unknown",
    }
    if record.get("batch_folder"):
        slim["batch_folder"] = record["batch_folder"]
    try:
        with _SCEPTER_REGISTRY_LOCK:
            data = {}
            if os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
            data[batch_id] = slim
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, path)
    except Exception as e:
        _scepter_log.warning("Could not persist SCEPTER batch registry %s: %s", path, e)


_load_scepter_batch_registry()


def _find_baseline_batch_for_member(job_id):
    """Resolve batch_id from a per-location baseline job_id using the registry/cache."""
    for bid, rec in BATCH_JOB_CACHE.items():
        if not str(bid).startswith("baseline_batch_"):
            continue
        jt = rec.get("job_type")
        if jt is not None and jt != "baseline":
            continue
        if job_id in (rec.get("job_ids") or []):
            return bid
    return None


def _effective_spinup_for_restart(spinup_name, spinup_batch_id=None):
    """
    restart_add_gbas looks for spinup under SCEPTER/jobs/<spinup_name>.
    Batch baseline outputs live at jobs/<baseline_batch_*>/<baseline_*_i>/,
    so pass spinup_name as baseline_batch_xxx/baseline_yyy_i (POSIX path under jobs/).
    """
    s = (spinup_name or "").strip()
    if not s or "/" in s:
        return s
    bid = None
    if spinup_batch_id:
        bid = str(spinup_batch_id).strip().strip("/")
    if not bid:
        info = JOB_STATUS_CACHE.get(s, {})
        bid = info.get("batch_id")
    if not bid:
        bid = _find_baseline_batch_for_member(s)
    if bid:
        return f"{bid}/{s}"
    return s


def _resolve_baseline_batch_id(batch_id):
    """
    Accept several frontend forms and return the canonical key in BATCH_JOB_CACHE.

    - baseline_batch_123456 (canonical)
    - baseline_12345_0 (any batch member job_id with batch_id in JOB_STATUS_CACHE)
    - 123456 -> baseline_batch_123456 (suffix only, if known to server)
    """
    if not batch_id or not str(batch_id).strip():
        return None, "Batch id is required."
    bid = str(batch_id).strip()

    if bid in BATCH_JOB_CACHE:
        return bid, None

    if bid.startswith("baseline_batch_"):
        return bid, None

    # Per-batch-location job_id: baseline_<5-digit-ts>_<index>
    if re.match(r"^baseline_\d+_\d+$", bid):
        info = JOB_STATUS_CACHE.get(bid, {})
        nested = info.get("batch_id")
        if nested and nested in BATCH_JOB_CACHE:
            return nested, None
        reg = _find_baseline_batch_for_member(bid)
        if reg:
            return reg, None
        return None, (
            f"No batch found for job {bid}. Use the batch_id from the submit response, "
            "or ensure scepter_batch_registry.json is present if the API was restarted."
        )

    # Bare numeric suffix only: 887986 -> baseline_batch_887986
    if bid.isdigit():
        cand = f"baseline_batch_{bid}"
        if cand in BATCH_JOB_CACHE:
            return cand, None
        return cand, None

    return None, (
        f"Invalid batch id '{bid}'. Expected baseline_batch_<suffix> from the batch submit "
        "response, or a batch member job id like baseline_<ts>_<index>."
    )


_SLURM_STATUS_MAP = {
    "PENDING": "pending",
    "RUNNING": "running",
    "COMPLETED": "completed",
    "COMPLETING": "running",
    "CONFIGURING": "pending",
    "FAILED": "failed",
    "CANCELLED": "failed",
    "TIMEOUT": "failed",
    "OUT_OF_MEMORY": "failed",
    "OUT_OF_MEMMORY": "failed",
    "OUT_OF_ME+": "failed",
}

# sacct -X State uses short codes on many clusters (e.g. CD=completed); squeue uses full words.
_SLURM_SACCT_COMPACT = {
    "CD": "completed",
    "CG": "running",
    "F": "failed",
    "NF": "failed",
    "CA": "failed",
    "TO": "failed",
    "OOM": "failed",
    "DL": "failed",
    "PD": "pending",
    "R": "running",
    "CF": "pending",
    "PR": "pending",
    "SI": "pending",
    "SE": "pending",
    "RV": "failed",
    "RF": "failed",
    "RH": "failed",
    "RQ": "failed",
    "RS": "failed",
}


def _slurm_raw_to_status(slurm_status):
    if not slurm_status:
        return "unknown"
    raw = slurm_status.strip()
    st_up = raw.upper()
    if st_up in _SLURM_SACCT_COMPACT:
        return _SLURM_SACCT_COMPACT[st_up]
    st = _SLURM_STATUS_MAP.get(raw, "unknown")
    if st == "unknown" and raw.startswith("OUT_OF_ME"):
        st = "failed"
    return st


def _slurm_sacct_resolve_state(ssh, bouchet_job_id):
    """Best State from sacct: prefer terminal (completed/failed) on any line.

    Runs both allocation-only (-X) and full step listing; merges lines. Job
    steps can show RUNNING on one line and COMPLETED on another; the first
    non-empty sacct query alone can miss the terminal state.
    """
    combined_lines = []
    for cmd in (
        f"sacct -j {bouchet_job_id} -n -X -o State -P 2>/dev/null",
        f"sacct -j {bouchet_job_id} -n -o State -P 2>/dev/null",
    ):
        stdin, stdout, stderr = ssh.exec_command(cmd)
        out = (stdout.read().decode() or "").strip()
        if not out:
            continue
        for ln in out.split("\n"):
            s = ln.strip()
            if s:
                combined_lines.append(s)
    if not combined_lines:
        return ""
    first_raw = None
    for line in combined_lines:
        raw = line.split("|")[0].strip()
        if not raw:
            continue
        if first_raw is None:
            first_raw = raw
        m = _slurm_raw_to_status(raw)
        if m in ("completed", "failed"):
            return raw
    return first_raw or ""


def _run_model_filesystem_completion(ssh, job_folder, restart_name):
    """True if the SLURM wrapper wrote .completed or the ERW run left run_complete.txt."""
    jf = shlex.quote(job_folder.rstrip("/"))
    stdin, stdout, stderr = ssh.exec_command(f"test -f {jf}/.completed && echo y")
    if (stdout.read().decode() or "").strip() == "y":
        return True
    if restart_name and isinstance(restart_name, str) and restart_name.strip():
        rel = restart_name.strip().lstrip("/")
        full = f"{job_folder.rstrip('/')}/{rel}"
        stdin, stdout, stderr = ssh.exec_command(
            f"test -f {shlex.quote(full)}/run_complete.txt && echo y"
        )
        if (stdout.read().decode() or "").strip() == "y":
            return True
    # Same layout as spinup heuristic: any run_complete under the job dir (cache may lack parameters)
    stdin, stdout, stderr = ssh.exec_command(
        f"find {jf} -maxdepth 4 -type f -name run_complete.txt 2>/dev/null | head -1"
    )
    return bool((stdout.read().decode() or "").strip())


def _batch_overall_from_status_counts(status_counts, n_jobs):
    if n_jobs == 0:
        return "unknown"
    if status_counts.get("completed", 0) == n_jobs:
        return "completed"
    if status_counts.get("failed", 0) > 0:
        return "failed"
    if status_counts.get("running", 0) > 0 or status_counts.get("pending", 0) > 0:
        return "running"
    if status_counts.get("submitting", 0) > 0 or status_counts.get("submitted", 0) > 0:
        return "submitting"
    return "unknown"


def _refresh_baseline_job_live(ssh, job_id, batch_folder=None):
    """
    Recompute baseline job status from Bouchet (squeue / .completed / sacct) and update JOB_STATUS_CACHE.
    Returns a dict suitable for batch status payloads.
    """
    job_info = dict(JOB_STATUS_CACHE.get(job_id, {}))
    if not job_id.startswith("baseline_"):
        return {
            "job_id": job_id,
            "status": "unknown",
            "bouchet_job_id": None,
            "error": "Invalid baseline job_id",
        }

    if job_info.get("status") == "failed" and job_info.get("error"):
        JOB_STATUS_CACHE[job_id] = job_info
        return {
            "job_id": job_id,
            "status": "failed",
            "bouchet_job_id": job_info.get("bouchet_job_id"),
            "error": job_info.get("error"),
        }

    if batch_folder:
        job_folder = f"{batch_folder.rstrip('/')}/{job_id}"
    else:
        job_folder = _resolve_baseline_job_folder_from_cache(job_id, job_info)
    job_info["job_folder"] = job_folder

    bouchet_job_id = job_info.get("bouchet_job_id")

    if not bouchet_job_id:
        check = f"test -d {job_folder} && echo exists || echo not_found"
        stdin, stdout, stderr = ssh.exec_command(check)
        if stdout.read().decode().strip() == "not_found":
            discovered = _discover_baseline_job_folder_ssh(ssh, job_id)
            if discovered:
                job_folder = discovered
                job_info["job_folder"] = job_folder
                JOB_STATUS_CACHE[job_id] = job_info
            elif job_info.get("status") == "submitting":
                job_info["status"] = "submitting"
                JOB_STATUS_CACHE[job_id] = job_info
                return {
                    "job_id": job_id,
                    "status": "submitting",
                    "bouchet_job_id": None,
                    "error": None,
                }
            else:
                job_info["status"] = "unknown"
                job_info["error"] = (
                    job_info.get("error") or "Job folder not found on Bouchet"
                )
                JOB_STATUS_CACHE[job_id] = job_info
                return {
                    "job_id": job_id,
                    "status": "unknown",
                    "bouchet_job_id": None,
                    "error": job_info.get("error"),
                }
        read_cmd = f"cat {job_folder}/.bouchet_job_id 2>/dev/null || (grep -h 'Submitted batch job' {job_folder}/*.out 2>/dev/null | tail -1 | awk '{{print $NF}}')"
        stdin, stdout, stderr = ssh.exec_command(read_cmd)
        recovered = stdout.read().decode().strip()
        if recovered:
            bouchet_job_id = recovered
            job_info["bouchet_job_id"] = bouchet_job_id
            JOB_STATUS_CACHE[job_id] = job_info
        else:
            check_done = f"test -f {job_folder}/.completed && echo completed || echo not_completed"
            stdin, stdout, stderr = ssh.exec_command(check_done)
            if stdout.read().decode().strip() == "completed":
                job_info["status"] = "completed"
                JOB_STATUS_CACHE[job_id] = job_info
                return {
                    "job_id": job_id,
                    "status": "completed",
                    "bouchet_job_id": None,
                    "error": None,
                }
            job_info["status"] = "submitting"
            JOB_STATUS_CACHE[job_id] = job_info
            return {
                "job_id": job_id,
                "status": "submitting",
                "bouchet_job_id": None,
                "error": None,
            }

    sacct_st = _slurm_sacct_resolve_state(ssh, bouchet_job_id)
    acct_m = _slurm_raw_to_status(sacct_st) if sacct_st else "unknown"

    if acct_m in ("completed", "failed"):
        status = acct_m
    else:
        squeue_cmd = f"squeue -j {bouchet_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status = (stdout.read().decode().strip().split("\n")[0] or "").strip()
        if slurm_status:
            status = _slurm_raw_to_status(slurm_status)
        else:
            check_done = (
                f"test -f {job_folder}/.completed && echo completed || echo not_completed"
            )
            stdin, stdout, stderr = ssh.exec_command(check_done)
            done = stdout.read().decode().strip() == "completed"
            status = "completed" if done else ("unknown" if not sacct_st else acct_m)

    if status == "running":
        stdin, stdout, stderr = ssh.exec_command(
            f"test -f {shlex.quote(job_folder.rstrip('/'))}/.completed && echo y"
        )
        if (stdout.read().decode() or "").strip() == "y":
            status = "completed"

    job_info["status"] = status
    JOB_STATUS_CACHE[job_id] = job_info
    return {
        "job_id": job_id,
        "status": status,
        "bouchet_job_id": bouchet_job_id,
        "error": job_info.get("error"),
    }


def _refresh_run_model_job_live(ssh, job_id):
    """Recompute run-model job status from Bouchet; update JOB_STATUS_CACHE."""
    job_info = dict(JOB_STATUS_CACHE.get(job_id, {}))
    if not job_id.startswith("scepter_run_"):
        return {
            "job_id": job_id,
            "status": "unknown",
            "bouchet_job_id": None,
            "error": "Invalid run-model job_id",
        }

    if job_info.get("status") == "failed" and job_info.get("error"):
        JOB_STATUS_CACHE[job_id] = job_info
        return {
            "job_id": job_id,
            "status": "failed",
            "bouchet_job_id": job_info.get("bouchet_job_id"),
            "error": job_info.get("error"),
        }

    job_folder = _resolve_run_model_job_folder(job_id, job_info)
    job_info["job_folder"] = job_folder
    bouchet_job_id = job_info.get("bouchet_job_id")

    if not bouchet_job_id:
        check = f"test -d {job_folder} && echo exists || echo not_found"
        stdin, stdout, stderr = ssh.exec_command(check)
        if stdout.read().decode().strip() == "not_found":
            if job_info.get("status") == "submitting":
                job_info["status"] = "submitting"
            else:
                job_info["status"] = "unknown"
                job_info["error"] = (
                    job_info.get("error") or "Job folder not found on Bouchet"
                )
            JOB_STATUS_CACHE[job_id] = job_info
            return {
                "job_id": job_id,
                "status": job_info["status"],
                "bouchet_job_id": None,
                "error": job_info.get("error"),
            }
        read_cmd = f"cat {job_folder}/.bouchet_job_id 2>/dev/null || (grep -h 'Submitted batch job' {job_folder}/*.out 2>/dev/null | tail -1 | awk '{{print $NF}}')"
        stdin, stdout, stderr = ssh.exec_command(read_cmd)
        recovered = stdout.read().decode().strip()
        if recovered:
            bouchet_job_id = recovered.split()[0].strip()
            job_info["bouchet_job_id"] = bouchet_job_id
            JOB_STATUS_CACHE[job_id] = job_info
        else:
            check_done = f"test -f {job_folder}/.completed && echo completed || echo not_completed"
            stdin, stdout, stderr = ssh.exec_command(check_done)
            if stdout.read().decode().strip() == "completed":
                job_info["status"] = "completed"
                JOB_STATUS_CACHE[job_id] = job_info
                return {
                    "job_id": job_id,
                    "status": "completed",
                    "bouchet_job_id": None,
                    "error": None,
                }
            job_info["status"] = "submitting"
            JOB_STATUS_CACHE[job_id] = job_info
            return {
                "job_id": job_id,
                "status": "submitting",
                "bouchet_job_id": None,
                "error": None,
            }

    bouchet_job_id = str(bouchet_job_id).strip().split()[0]

    sacct_st = _slurm_sacct_resolve_state(ssh, bouchet_job_id)
    acct_m = _slurm_raw_to_status(sacct_st) if sacct_st else "unknown"

    if acct_m in ("completed", "failed"):
        status = acct_m
    else:
        squeue_cmd = f"squeue -j {bouchet_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status = (stdout.read().decode().strip().split("\n")[0] or "").strip()
        if slurm_status:
            status = _slurm_raw_to_status(slurm_status)
        else:
            check_done = (
                f"test -f {job_folder}/.completed && echo completed || echo not_completed"
            )
            stdin, stdout, stderr = ssh.exec_command(check_done)
            done = stdout.read().decode().strip() == "completed"
            status = "completed" if done else ("unknown" if not sacct_st else acct_m)

    if status == "running":
        params = job_info.get("parameters") or {}
        rn = params.get("restart_name") or params.get("restartName")
        if _run_model_filesystem_completion(ssh, job_folder, rn):
            status = "completed"

    job_info["status"] = status
    JOB_STATUS_CACHE[job_id] = job_info
    return {
        "job_id": job_id,
        "status": status,
        "bouchet_job_id": bouchet_job_id,
        "error": job_info.get("error"),
    }


def _baseline_jobs_root():
    return f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs"


def _resolve_run_model_job_folder(job_id, job_info):
    """Canonical jobs/.../scepter_run_* path (matches submit_run_scepter_model)."""
    if job_info.get("job_folder"):
        return job_info["job_folder"]
    return f"{_baseline_jobs_root()}/{job_id}"


def _resolve_baseline_job_folder_from_cache(job_id, job_info):
    """
    Baseline batch jobs live under jobs/baseline_batch_*/baseline_*_i/.
    Single-job submit uses jobs/baseline_*.
    """
    jf = job_info.get("job_folder")
    if jf:
        return jf
    bid = job_info.get("batch_id")
    if bid:
        batch_folder = BATCH_JOB_CACHE.get(bid, {}).get("batch_folder")
        if batch_folder:
            return f"{batch_folder.rstrip('/')}/{job_id}"
    return f"{_baseline_jobs_root()}/{job_id}"


def _discover_baseline_job_folder_ssh(ssh, job_id):
    """Locate job directory on Bouchet (handles batch nesting when cache is stale)."""
    root = _baseline_jobs_root()
    cmd = f"find {root} -maxdepth 3 -type d -name '{job_id}' 2>/dev/null | head -1"
    stdin, stdout, stderr = ssh.exec_command(cmd)
    return stdout.read().decode().strip()


# ---------------------------------------------------------------------------
# Baseline simulation (create_spinup_slurm_jobs.py)
# ---------------------------------------------------------------------------


@scepter_bp.route("/api/baseline-simulation", methods=["OPTIONS", "POST"])
def submit_baseline_simulation():
    """
    Run baseline simulation: python3 create_spinup_slurm_jobs.py on Bouchet.
    The script reads parameters from {job_folder}/parameters.json (written by backend).

    Payload: { "coordinate": [lat, lon], "location_name": "..." } or { "latitude": lat, "longitude": lon }
    """
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        if (
            not os.getenv("BOUCHET_HOST")
            or not os.getenv("BOUCHET_USER")
            or not os.getenv("SSH_PRIVATE_KEY")
        ):
            raise Exception(
                "BOUCHET_HOST, BOUCHET_USER, and SSH_PRIVATE_KEY must be set"
            )

        payload = request.get_json(silent=True) or {}
        coordinate = payload.get("coordinate") or payload.get("coordinates")
        if coordinate and isinstance(coordinate, list) and len(coordinate) >= 2:
            lat, lon = float(coordinate[0]), float(coordinate[1])
        elif "latitude" in payload and "longitude" in payload:
            lat, lon = float(payload["latitude"]), float(payload["longitude"])
        else:
            return (
                jsonify(
                    {"error": "coordinate [lat, lon] or latitude/longitude required"}
                ),
                400,
            )

        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return jsonify({"error": "Coordinates out of valid range"}), 400

        location_name = payload.get("location_name") or payload.get("locationName")

        timestamp = int(time.time())
        job_id = f"baseline_{str(timestamp)[-5:]}"
        scepter_path = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER"
        job_folder = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}"
        create_spinup_script = os.getenv(
            "CREATE_SPINUP_SLURM_SCRIPT_PATH",
            f"{scepter_path}/create_spinup_slurm_jobs.py",
        )

        output_dir = f"{job_folder}/output"
        JOB_STATUS_CACHE[job_id] = {
            "job_id": job_id,
            "bouchet_job_id": None,
            "job_folder": job_folder,
            "output_dir": output_dir,
            "job_type": "baseline",
            "parameters": {
                "coordinate": [lat, lon],
                "location_name": location_name,
            },
            "status": "submitting",
            "submitted_at": time.time(),
        }

        app = current_app._get_current_object()

        def submit_baseline_background():
            with app.app_context():
                try:
                    current_app.logger.info(
                        f"Starting baseline simulation job {job_id}"
                    )
                    ssh = get_ssh_connection_pooled()

                    output_dir = f"{job_folder}/output"
                    params_data = {
                        "coordinate": [lat, lon],
                        "location_name": location_name,
                        "job_folder": job_folder,
                        "output_dir": output_dir,
                        "job_id": job_id,
                    }
                    params_json = json.dumps(params_data, indent=2)
                    commands = [
                        f"mkdir -p {job_folder}",
                        f"mkdir -p {output_dir}",
                        f"cat > {job_folder}/parameters.json << 'PARAMS_EOF'\n{params_json}\nPARAMS_EOF",
                        f"echo '{lat} {lon}' > {job_folder}/coords.txt",
                    ]
                    for cmd in commands:
                        stdin, stdout, stderr = ssh.exec_command(cmd)
                        if stdout.channel.recv_exit_status() != 0:
                            err = stderr.read().decode()
                            JOB_STATUS_CACHE[job_id].update(
                                {"status": "failed", "error": err}
                            )
                            return

                    sbatch_script = f"""#!/bin/bash
#SBATCH --job-name={job_id[:12]}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output={job_folder}/%x_%j.out
#SBATCH --error={job_folder}/%x_%j.err

set -x

cd {scepter_path}

# Use a venv with numpy/deps if present (spinup.py needs numpy, make_inputs, etc.)
for venv_path in ".venv" "venv" "../venv"; do
    if [ -f "$venv_path/bin/activate" ]; then
        if "$venv_path/bin/python" -c "import numpy" 2>/dev/null; then
            echo "Activating venv at $venv_path (numpy OK)"
            set +u
            source "$venv_path/bin/activate"
            set -u
            break
        fi
    fi
done

# So Python can find the spinup module (e.g. spinup.py or spinup/ inside SCEPTER)
export PYTHONPATH="{scepter_path}:{job_folder}:${{PYTHONPATH:-}}"
export SCEPTER_ROOT="{scepter_path}"

# Step 1: create_spinup_slurm_jobs.py writes parameters and creates the main spinup script (e.g. spinup_name.sh)
python3 {create_spinup_script} {job_folder}
EXIT=$?
if [ $EXIT -ne 0 ]; then
    echo "ERROR: create_spinup_slurm_jobs.py failed with exit $EXIT"
    exit $EXIT
fi

# Step 2: Run the main spinup script from SCEPTER dir so make/./data resolve (SCEPTER_ROOT already set)
for f in {job_folder}/*.sh; do
    [ -f "$f" ] || continue
    base="$(basename "$f")"
    [ "$base" = "job.sh" ] && continue
    echo "Running main spinup script: $base (cwd=$SCEPTER_ROOT)"
    (cd "$SCEPTER_ROOT" && bash "$f") || exit 1
done

echo "Job completed at $(date)" > {job_folder}/.completed
exit 0
"""

                    script_path = f"{job_folder}/job.sh"
                    stdin, stdout, stderr = ssh.exec_command(
                        f"cat > {script_path} << 'SCRIPT_EOF'\n{sbatch_script}\nSCRIPT_EOF"
                    )
                    if stdout.channel.recv_exit_status() != 0:
                        err = stderr.read().decode()
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "status": "failed",
                                "error": f"Failed to write script: {err}",
                            }
                        )
                        return

                    ssh.exec_command(f"chmod +x {script_path}")
                    stdout.channel.recv_exit_status()

                    stdin, stdout, stderr = ssh.exec_command(f"sbatch {script_path}")
                    result = stdout.read().decode().strip()
                    error = stderr.read().decode().strip()
                    exit_status = stdout.channel.recv_exit_status()

                    if exit_status == 0 and "Submitted batch job" in result:
                        bouchet_job_id = result.split()[-1]
                        try:
                            stdin2, stdout2, stderr2 = ssh.exec_command(
                                f"echo '{bouchet_job_id}' > {job_folder}/.bouchet_job_id"
                            )
                            stdout2.channel.recv_exit_status()
                        except Exception:
                            pass
                        JOB_STATUS_CACHE[job_id].update(
                            {"bouchet_job_id": bouchet_job_id, "status": "submitted"}
                        )
                        current_app.logger.info(
                            f"Baseline job {job_id} submitted with Bouchet ID {bouchet_job_id}"
                        )
                    else:
                        JOB_STATUS_CACHE[job_id].update(
                            {"status": "failed", "error": error or "sbatch failed"}
                        )
                        current_app.logger.error(
                            f"Baseline job {job_id} failed: {error}"
                        )
                except Exception as e:
                    import traceback

                    current_app.logger.error(
                        f"Baseline submission error for {job_id}: {str(e)}"
                    )
                    current_app.logger.error(traceback.format_exc())
                    JOB_STATUS_CACHE[job_id].update(
                        {"status": "failed", "error": str(e)}
                    )

        threading.Thread(target=submit_baseline_background, daemon=True).start()

        return jsonify(
            {
                "job_id": job_id,
                "bouchet_job_id": None,
                "status": "submitting",
                "message": f"Baseline simulation job is being submitted. Job ID: {job_id}.",
                "parameters": {"coordinate": [lat, lon]},
            }
        )

    except Exception as e:
        import traceback

        current_app.logger.error(f"Error in submit_baseline_simulation: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@scepter_bp.route("/api/baseline-simulation-batch", methods=["OPTIONS", "POST"])
@scepter_bp.route("/api/scepter/baseline-simulation-batch", methods=["OPTIONS", "POST"])
def submit_baseline_simulation_batch():
    """
    Submit baseline spinup for multiple locations.
    Each location is submitted as a separate SLURM job.
    """
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        if (
            not os.getenv("BOUCHET_HOST")
            or not os.getenv("BOUCHET_USER")
            or not os.getenv("SSH_PRIVATE_KEY")
        ):
            raise Exception(
                "BOUCHET_HOST, BOUCHET_USER, and SSH_PRIVATE_KEY must be set"
            )

        payload = request.get_json(silent=True) or {}
        coordinates = payload.get("coordinates") or payload.get("locations")
        location_names = (
            payload.get("location_names") or payload.get("locationNames") or []
        )

        if not coordinates or not isinstance(coordinates, list):
            return jsonify({"error": "coordinates array is required"}), 400

        timestamp = int(time.time())
        batch_id = f"baseline_batch_{str(timestamp)[-6:]}"
        jobs_base = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs"
        batch_folder = f"{jobs_base}/{batch_id}"
        scepter_path = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER"
        create_spinup_script = os.getenv(
            "CREATE_SPINUP_SLURM_SCRIPT_PATH",
            f"{scepter_path}/create_spinup_slurm_jobs.py",
        )

        job_ids = []
        job_configs = []
        for i, coord in enumerate(coordinates):
            if not isinstance(coord, list) or len(coord) < 2:
                return jsonify({"error": f"coordinates[{i}] must be [lat, lon]"}), 400

            lat, lon = float(coord[0]), float(coord[1])
            if lat < -90 or lat > 90 or lon < -180 or lon > 180:
                return jsonify({"error": f"coordinates[{i}] out of valid range"}), 400

            location_name = None
            if isinstance(location_names, list) and i < len(location_names):
                location_name = location_names[i]

            job_id = f"baseline_{str(timestamp)[-5:]}_{i}"
            job_folder = f"{batch_folder}/{job_id}"
            output_dir = f"{job_folder}/output"

            JOB_STATUS_CACHE[job_id] = {
                "job_id": job_id,
                "bouchet_job_id": None,
                "job_folder": job_folder,
                "output_dir": output_dir,
                "job_type": "baseline",
                "parameters": {
                    "coordinate": [lat, lon],
                    "location_name": location_name,
                },
                "status": "submitting",
                "submitted_at": time.time(),
                "batch_id": batch_id,
            }

            job_ids.append(job_id)
            job_configs.append(
                {
                    "job_id": job_id,
                    "job_folder": job_folder,
                    "output_dir": output_dir,
                    "lat": lat,
                    "lon": lon,
                    "location_name": location_name,
                }
            )

        BATCH_JOB_CACHE[batch_id] = {
            "batch_id": batch_id,
            "job_ids": job_ids,
            "submitted_at": time.time(),
            "job_type": "baseline",
            "batch_folder": batch_folder,
        }
        _persist_scepter_batch_record(batch_id, BATCH_JOB_CACHE[batch_id])

        app = current_app._get_current_object()

        def submit_baseline_batch_background():
            with app.app_context():
                ssh = None
                try:
                    ssh = get_ssh_connection_pooled()
                    manifest_data = {
                        "batch_id": batch_id,
                        "job_ids": job_ids,
                        "coordinates": coordinates,
                        "location_names": (
                            location_names if isinstance(location_names, list) else []
                        ),
                        "submitted_at": time.time(),
                    }
                    manifest_json = json.dumps(manifest_data, indent=2)
                    ssh.exec_command(f"mkdir -p {batch_folder}")
                    ssh.exec_command(
                        f"cat > {batch_folder}/manifest.json << 'MANIFEST_EOF'\n{manifest_json}\nMANIFEST_EOF"
                    )
                    for cfg in job_configs:
                        try:
                            current_app.logger.info(
                                f"Starting baseline simulation job {cfg['job_id']} (batch {batch_id})"
                            )
                            params_data = {
                                "coordinate": [cfg["lat"], cfg["lon"]],
                                "location_name": cfg["location_name"],
                                "job_folder": cfg["job_folder"],
                                "output_dir": cfg["output_dir"],
                                "job_id": cfg["job_id"],
                            }
                            params_json = json.dumps(params_data, indent=2)
                            commands = [
                                f"mkdir -p {cfg['job_folder']}",
                                f"mkdir -p {cfg['output_dir']}",
                                f"cat > {cfg['job_folder']}/parameters.json << 'PARAMS_EOF'\n{params_json}\nPARAMS_EOF",
                                f"echo '{cfg['lat']} {cfg['lon']}' > {cfg['job_folder']}/coords.txt",
                            ]
                            failed = False
                            for cmd in commands:
                                stdin, stdout, stderr = ssh.exec_command(cmd)
                                if stdout.channel.recv_exit_status() != 0:
                                    err = stderr.read().decode()
                                    JOB_STATUS_CACHE[cfg["job_id"]].update(
                                        {"status": "failed", "error": err}
                                    )
                                    failed = True
                                    break
                            if failed:
                                continue

                            sbatch_script = f"""#!/bin/bash
#SBATCH --job-name={cfg["job_id"][:12]}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output={cfg["job_folder"]}/%x_%j.out
#SBATCH --error={cfg["job_folder"]}/%x_%j.err

set -x

cd {scepter_path}

for venv_path in ".venv" "venv" "../venv"; do
    if [ -f "$venv_path/bin/activate" ]; then
        if "$venv_path/bin/python" -c "import numpy" 2>/dev/null; then
            echo "Activating venv at $venv_path (numpy OK)"
            set +u
            source "$venv_path/bin/activate"
            set -u
            break
        fi
    fi
done

export PYTHONPATH="{scepter_path}:{cfg["job_folder"]}:${{PYTHONPATH:-}}"
export SCEPTER_ROOT="{scepter_path}"

python3 {create_spinup_script} {cfg["job_folder"]}
EXIT=$?
if [ $EXIT -ne 0 ]; then
    echo "ERROR: create_spinup_slurm_jobs.py failed with exit $EXIT"
    exit $EXIT
fi

for f in {cfg["job_folder"]}/*.sh; do
    [ -f "$f" ] || continue
    base="$(basename "$f")"
    [ "$base" = "job.sh" ] && continue
    echo "Running main spinup script: $base (cwd=$SCEPTER_ROOT)"
    (cd "$SCEPTER_ROOT" && bash "$f") || exit 1
done

echo "Job completed at $(date)" > {cfg["job_folder"]}/.completed
exit 0
"""

                            script_path = f"{cfg['job_folder']}/job.sh"
                            stdin, stdout, stderr = ssh.exec_command(
                                f"cat > {script_path} << 'SCRIPT_EOF'\n{sbatch_script}\nSCRIPT_EOF"
                            )
                            if stdout.channel.recv_exit_status() != 0:
                                err = stderr.read().decode()
                                JOB_STATUS_CACHE[cfg["job_id"]].update(
                                    {
                                        "status": "failed",
                                        "error": f"Failed to write script: {err}",
                                    }
                                )
                                continue

                            ssh.exec_command(f"chmod +x {script_path}")
                            stdout.channel.recv_exit_status()

                            stdin, stdout, stderr = ssh.exec_command(
                                f"sbatch {script_path}"
                            )
                            result = stdout.read().decode().strip()
                            error = stderr.read().decode().strip()
                            exit_status = stdout.channel.recv_exit_status()

                            if exit_status == 0 and "Submitted batch job" in result:
                                bouchet_job_id = result.split()[-1]
                                try:
                                    stdin2, stdout2, stderr2 = ssh.exec_command(
                                        f"echo '{bouchet_job_id}' > {cfg['job_folder']}/.bouchet_job_id"
                                    )
                                    stdout2.channel.recv_exit_status()
                                except Exception:
                                    pass
                                JOB_STATUS_CACHE[cfg["job_id"]].update(
                                    {
                                        "bouchet_job_id": bouchet_job_id,
                                        "status": "submitted",
                                    }
                                )
                            else:
                                JOB_STATUS_CACHE[cfg["job_id"]].update(
                                    {
                                        "status": "failed",
                                        "error": error or "sbatch failed",
                                    }
                                )
                        except Exception as e:
                            JOB_STATUS_CACHE[cfg["job_id"]].update(
                                {"status": "failed", "error": str(e)}
                            )
                except Exception as e:
                    current_app.logger.error(
                        f"Baseline batch submission error for {batch_id}: {str(e)}"
                    )
                finally:
                    if ssh:
                        try:
                            ssh.close()
                        except Exception:
                            pass

        threading.Thread(target=submit_baseline_batch_background, daemon=True).start()

        return jsonify(
            {
                "batch_id": batch_id,
                "job_ids": job_ids,
                "status": "submitting",
                "message": f"Submitting {len(job_ids)} baseline simulation job(s). Each location is a separate SLURM job.",
                "location_count": len(job_ids),
            }
        )
    except Exception as e:
        current_app.logger.error(f"Error in submit_baseline_simulation_batch: {str(e)}")
        return jsonify({"error": str(e)}), 500


@scepter_bp.route(
    "/api/baseline-simulation-batch/<batch_id>/status", methods=["GET", "OPTIONS"]
)
@scepter_bp.route(
    "/api/scepter/baseline-simulation-batch/<batch_id>/status",
    methods=["GET", "OPTIONS"],
)
def check_baseline_simulation_batch_status(batch_id):
    """Check status for a baseline-simulation batch (in-memory cache only; no SSH)."""
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    resolved, err = _resolve_baseline_batch_id(batch_id)
    if err and not resolved:
        return jsonify({"error": err, "batch_id": batch_id}), 400

    batch_info = BATCH_JOB_CACHE.get(resolved, {})
    job_ids = batch_info.get("job_ids", [])
    if not resolved.startswith("baseline_batch_"):
        return jsonify({"error": "Invalid batch_id", "batch_id": batch_id}), 400
    if not job_ids:
        return (
            jsonify(
                {
                    "error": "Batch not found in server memory. "
                    "Use the batch_id returned by POST /api/baseline-simulation-batch. "
                    "If the server restarted, re-submit or poll per-job status using full job_id.",
                    "batch_id": resolved,
                    "hint": resolved,
                }
            ),
            404,
        )

    jobs_status = []
    status_counts = {}
    for jid in job_ids:
        info = JOB_STATUS_CACHE.get(jid, {})
        st = info.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1
        jobs_status.append(
            {
                "job_id": jid,
                "status": st,
                "bouchet_job_id": info.get("bouchet_job_id"),
                "error": info.get("error"),
            }
        )

    overall = _batch_overall_from_status_counts(status_counts, len(job_ids))

    return jsonify(
        {
            "batch_id": resolved,
            "overall": overall,
            "jobs": jobs_status,
            "status_counts": status_counts,
            "submitted_at": batch_info.get("submitted_at"),
        }
    )


@scepter_bp.route(
    "/api/baseline-simulation-batch/<batch_id>/download", methods=["GET", "OPTIONS"]
)
@scepter_bp.route(
    "/api/scepter/baseline-simulation-batch/<batch_id>/download",
    methods=["GET", "OPTIONS"],
)
def download_baseline_simulation_batch_results(batch_id):
    """Download a whole baseline batch folder as zip."""
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        resolved, err = _resolve_baseline_batch_id(batch_id)
        if err and not resolved:
            return jsonify({"error": err, "batch_id": batch_id}), 400
        if not resolved.startswith("baseline_batch_"):
            return jsonify({"error": "Invalid batch_id", "batch_id": batch_id}), 400

        batch_info = BATCH_JOB_CACHE.get(resolved, {})
        batch_folder = batch_info.get("batch_folder") or (
            f"{_baseline_jobs_root()}/{resolved}"
        )

        try:
            ssh = get_ssh_connection_pooled()
        except Exception as ssh_error:
            error_msg = str(ssh_error)
            current_app.logger.error(
                f"SSH connection failed for baseline batch download {batch_id}: {error_msg}"
            )
            if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
                return (
                    jsonify(
                        {
                            "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                            "batch_id": batch_id,
                        }
                    ),
                    429,
                )
            return (
                jsonify(
                    {
                        "error": f"Failed to connect to Bouchet HPC: {error_msg}",
                        "batch_id": batch_id,
                    }
                ),
                500,
            )

        chk = f"test -d {batch_folder} && echo ok || echo missing"
        stdin, stdout, stderr = ssh.exec_command(chk)
        if stdout.read().decode().strip() != "ok":
            return (
                jsonify(
                    {
                        "error": "Batch folder not found on Bouchet",
                        "batch_id": resolved,
                        "batch_folder": batch_folder,
                    }
                ),
                404,
            )

        parent_dir = os.path.dirname(batch_folder.rstrip("/")) or "."
        folder_name = os.path.basename(batch_folder.rstrip("/"))
        zip_cmd = f"cd {parent_dir} && zip -r {folder_name}.zip {folder_name}/ 2>&1"
        stdin, stdout, stderr = ssh.exec_command(zip_cmd, timeout=180)
        exit_status = stdout.channel.recv_exit_status()
        zip_output = stdout.read().decode()
        error_msg = stderr.read().decode()

        if exit_status != 0:
            current_app.logger.error(
                f"Failed to create baseline batch zip for {batch_id}: {error_msg or zip_output}"
            )
            return (
                jsonify(
                    {
                        "error": f"Failed to create zip on Bouchet: {error_msg or zip_output}",
                        "batch_id": batch_id,
                    }
                ),
                500,
            )

        sftp = ssh.open_sftp()
        zip_path = f"{parent_dir}/{folder_name}.zip"
        remote_file = sftp.open(zip_path, "rb")
        zip_data = remote_file.read()
        remote_file.close()
        sftp.close()

        response = make_response(zip_data)
        response.headers["Content-Type"] = "application/zip"
        response.headers["Content-Disposition"] = (
            f"attachment; filename={folder_name}.zip"
        )
        return response

    except Exception as e:
        error_msg = str(e)
        current_app.logger.error(
            f"Error in download_baseline_simulation_batch_results for {batch_id}: {error_msg}"
        )
        if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
            return (
                jsonify(
                    {
                        "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                        "batch_id": batch_id,
                    }
                ),
                429,
            )
        return (
            jsonify(
                {"error": f"Failed to download baseline batch results: {error_msg}"}
            ),
            500,
        )


@scepter_bp.route(
    "/api/baseline-simulation/<job_id>/status", methods=["GET", "OPTIONS"]
)
@scepter_bp.route(
    "/api/scepter/baseline-simulation/<job_id>/status", methods=["GET", "OPTIONS"]
)
def check_baseline_simulation_status(job_id):
    """Check status of a baseline simulation job"""
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        job_info = dict(JOB_STATUS_CACHE.get(job_id, {}))
        if not job_id.startswith("baseline_"):
            return jsonify({"error": "Invalid baseline job_id", "job_id": job_id}), 400

        if job_info.get("status") == "failed":
            return jsonify(
                {
                    "job_id": job_id,
                    "status": "failed",
                    "error": job_info.get("error", "Job failed"),
                }
            )

        job_folder = _resolve_baseline_job_folder_from_cache(job_id, job_info)
        job_info["job_folder"] = job_folder
        JOB_STATUS_CACHE[job_id] = job_info
        bouchet_job_id = job_info.get("bouchet_job_id")

        try:
            ssh = get_ssh_connection_pooled()
        except Exception as e:
            # SSH failed (e.g. Duo timeout); return cached status if we have one
            if bouchet_job_id or job_info:
                cached = job_info.get("status", "submitted")
                return jsonify(
                    {
                        "job_id": job_id,
                        "bouchet_job_id": bouchet_job_id,
                        "status": cached,
                        "message": "Could not verify status (SSH unavailable). Using cached status.",
                        "error": str(e),
                    }
                )
            return (
                jsonify({"job_id": job_id, "status": "unknown", "error": str(e)}),
                500,
            )

        row = _refresh_baseline_job_live(ssh, job_id)
        status = row["status"]
        bouchet_job_id = row.get("bouchet_job_id")
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder",
            f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}",
        )

        logs = []
        if status in ["running", "completed", "failed"]:
            log_cmd = f"tail -n 50 {job_folder}/*.out 2>/dev/null; tail -n 20 {job_folder}/*.err 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()
            if log_content:
                logs = log_content.split("\n")[-15:]

        return jsonify(
            {
                "job_id": job_id,
                "bouchet_job_id": bouchet_job_id,
                "status": status,
                "submitted_at": job_info.get("submitted_at"),
                "logs": logs,
            }
        )

    except Exception as e:
        import traceback

        current_app.logger.error(
            f"Error in check_baseline_simulation_status for {job_id}: {str(e)}"
        )
        current_app.logger.error(traceback.format_exc())
        return jsonify({"job_id": job_id, "status": "unknown", "error": str(e)}), 500


@scepter_bp.route(
    "/api/baseline-simulation/<job_id>/download", methods=["GET", "OPTIONS"]
)
@scepter_bp.route(
    "/api/scepter/baseline-simulation/<job_id>/download", methods=["GET", "OPTIONS"]
)
def download_baseline_simulation_results(job_id):
    """Download baseline simulation results as a zip file.

    On Bouchet, baseline outputs live in the baseline_* job folder.
    This endpoint zips the entire baseline_* folder rather than just the output subdirectory.
    """
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        import io

        job_info = dict(JOB_STATUS_CACHE.get(job_id, {}))
        job_folder = _resolve_baseline_job_folder_from_cache(job_id, job_info)

        try:
            ssh = get_ssh_connection_pooled()
        except Exception as ssh_error:
            error_msg = str(ssh_error)
            current_app.logger.error(
                f"SSH connection failed for baseline download {job_id}: {error_msg}"
            )
            if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
                return (
                    jsonify(
                        {
                            "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                            "job_id": job_id,
                        }
                    ),
                    429,
                )
            return (
                jsonify(
                    {
                        "error": f"Failed to connect to Bouchet HPC: {error_msg}",
                        "job_id": job_id,
                    }
                ),
                500,
            )

        check = f"test -d {job_folder} && echo exists || echo not_found"
        stdin, stdout, stderr = ssh.exec_command(check)
        if stdout.read().decode().strip() == "not_found":
            discovered = _discover_baseline_job_folder_ssh(ssh, job_id)
            if discovered:
                job_folder = discovered
                job_info["job_folder"] = job_folder
                JOB_STATUS_CACHE[job_id] = job_info
            else:
                return jsonify({"error": "Job folder not found", "job_id": job_id}), 404

        # Create a zip of the baseline_* folder on Bouchet.
        # This will work even if the job is still running; it zips whatever files exist.
        parent_dir = os.path.dirname(job_folder.rstrip("/")) or "."
        folder_name = os.path.basename(job_folder.rstrip("/"))
        zip_cmd = f"cd {parent_dir} && zip -r {folder_name}.zip {folder_name}/ 2>&1"
        stdin, stdout, stderr = ssh.exec_command(zip_cmd, timeout=120)
        exit_status = stdout.channel.recv_exit_status()
        zip_output = stdout.read().decode()
        error_msg = stderr.read().decode()

        if exit_status != 0:
            current_app.logger.error(
                f"Failed to create baseline zip for {job_id}: {error_msg or zip_output}"
            )
            return (
                jsonify(
                    {
                        "error": f"Failed to create zip on Bouchet: {error_msg or zip_output}",
                        "job_id": job_id,
                    }
                ),
                500,
            )

        # Download the zip file over SFTP into memory
        sftp = ssh.open_sftp()
        zip_path = f"{parent_dir}/{folder_name}.zip"
        remote_file = sftp.open(zip_path, "rb")
        zip_data = remote_file.read()
        remote_file.close()
        sftp.close()

        response = make_response(zip_data)
        response.headers["Content-Type"] = "application/zip"
        response.headers["Content-Disposition"] = (
            f"attachment; filename={folder_name}.zip"
        )
        return response

    except Exception as e:
        error_msg = str(e)
        current_app.logger.error(
            f"Error in download_baseline_simulation_results for {job_id}: {error_msg}"
        )
        import traceback

        current_app.logger.error(f"Traceback: {traceback.format_exc()}")

        if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
            return (
                jsonify(
                    {
                        "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                        "job_id": job_id,
                    }
                ),
                429,
            )

        return (
            jsonify({"error": f"Failed to download baseline results: {error_msg}"}),
            500,
        )


# ---------------------------------------------------------------------------
# Run SCEPTER model (restart_add_gbas.py)
# ---------------------------------------------------------------------------


@scepter_bp.route("/api/run-scepter-model", methods=["OPTIONS", "POST"])
@scepter_bp.route("/api/scepter/run-model", methods=["OPTIONS", "POST"])
def submit_run_scepter_model():
    """
    Run SCEPTER model: python3 restart_add_gbas.py <spinup_name> <restart_name> on Bouchet.
    spinup_name comes from the baseline simulation (e.g. baseline job_id).
    Payload: {
        "spinup_name": "...", "restart_name": "...",
        "particle_size": <required>,
        "application_rate": <required>,  # ton/ha/yr
        "target_pH": <optional>
    }
    """
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        if (
            not os.getenv("BOUCHET_HOST")
            or not os.getenv("BOUCHET_USER")
            or not os.getenv("SSH_PRIVATE_KEY")
        ):
            raise Exception(
                "BOUCHET_HOST, BOUCHET_USER, and SSH_PRIVATE_KEY must be set"
            )

        payload = request.get_json(silent=True) or {}
        spinup_name = payload.get("spinup_name") or payload.get("spinupName")
        restart_name = payload.get("restart_name") or payload.get("restartName")
        target_pH = (
            payload.get("target_pH")
            or payload.get("targetpH")
            or payload.get("target_pH_arg")
        )

        if (
            not spinup_name
            or not isinstance(spinup_name, str)
            or not spinup_name.strip()
        ):
            return (
                jsonify(
                    {"error": "spinup_name is required (from baseline simulation)"}
                ),
                400,
            )
        if (
            not restart_name
            or not isinstance(restart_name, str)
            or not restart_name.strip()
        ):
            return jsonify({"error": "restart_name is required"}), 400

        particle_size = (
            payload.get("particle_size")
            or payload.get("particleSize")
            or payload.get("site_p80")
        )
        application_rate = (
            payload.get("application_rate")
            or payload.get("applicationRate")
            or payload.get("site_app_rate_tha")
        )
        if particle_size is None:
            return jsonify({"error": "particle_size is required"}), 400
        if application_rate is None:
            return jsonify({"error": "application_rate is required"}), 400

        spinup_name = spinup_name.strip()
        restart_name = restart_name.strip()
        spinup_batch_id = (
            payload.get("spinup_batch_id")
            or payload.get("baseline_batch_id")
            or payload.get("spinupBatchId")
            or payload.get("baselineBatchId")
        )
        spinup_for_restart = _effective_spinup_for_restart(
            spinup_name, spinup_batch_id=spinup_batch_id
        )

        # Convert application_rate from ton/ha/yr to g/m²/yr (1 ton/ha = 100 g/m²)
        application_rate_tha = float(application_rate)
        application_rate_g_m2_yr = application_rate_tha * 100.0

        # Optional: target_pH
        params_extras = {
            "particle_size": particle_size,
            "application_rate": application_rate_g_m2_yr,  # g/m²/yr
        }
        if target_pH is not None and str(target_pH).strip():
            params_extras["target_pH"] = target_pH
        else:
            params_extras["target_pH"] = "(not set)"

        timestamp = int(time.time())
        job_id = f"scepter_run_{str(timestamp)[-5:]}"
        scepter_path = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER"
        job_folder = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}"
        restart_script = os.getenv(
            "RESTART_ADD_GBAS_SCRIPT", f"{scepter_path}/restart_add_gbas.py"
        )

        JOB_STATUS_CACHE[job_id] = {
            "job_id": job_id,
            "bouchet_job_id": None,
            "job_folder": job_folder,
            "job_type": "scepter_run",
            "parameters": {
                "spinup_name": spinup_name,
                "spinup_for_restart": spinup_for_restart,
                "restart_name": restart_name,
                **params_extras,
            },
            "status": "submitting",
            "submitted_at": time.time(),
        }

        app = current_app._get_current_object()

        def submit_run_model_background():
            with app.app_context():
                try:
                    current_app.logger.info(f"Starting SCEPTER run-model job {job_id}")
                    ssh = get_ssh_connection()

                    params_data = {
                        "spinup_name": spinup_name,
                        "spinup_for_restart": spinup_for_restart,
                        "restart_name": restart_name,
                        "particle_size": params_extras.get("particle_size"),
                        "application_rate": params_extras.get(
                            "application_rate"
                        ),  # g/m²/yr
                        "target_pH": params_extras.get("target_pH", "(not set)"),
                    }
                    params_json = json.dumps(params_data, indent=2)
                    commands = [
                        f"mkdir -p {job_folder}",
                        f"echo '{spinup_for_restart}' > {job_folder}/spinup_name.txt",
                        f"echo '{restart_name}' > {job_folder}/restart_name.txt",
                        f"cat > {job_folder}/parameters.json << 'PARAMS_EOF'\n{params_json}\nPARAMS_EOF",
                    ]
                    for cmd in commands:
                        stdin, stdout, stderr = ssh.exec_command(cmd)
                        if stdout.channel.recv_exit_status() != 0:
                            err = stderr.read().decode()
                            ssh.close()
                            JOB_STATUS_CACHE[job_id].update(
                                {"status": "failed", "error": err}
                            )
                            return

                    scepter_jobs_dir = f"{scepter_path.rstrip('/')}/jobs/"
                    restart_dir_full = f"{job_folder.rstrip('/')}/{restart_name}"

                    sbatch_script = f"""#!/bin/bash
#SBATCH --job-name={job_id[:12]}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output={job_folder}/%x_%j.out
#SBATCH --error={job_folder}/%x_%j.err

set -x
cd {scepter_path}

export SCEPTER_JOBS_DIR={shlex.quote(scepter_jobs_dir)}
export SCEPTER_RESTART_DIR={shlex.quote(restart_dir_full)}

# Use a venv with numpy if present (restart_add_gbas.py needs numpy)
for venv_path in ".venv" "venv" "../venv"; do
    if [ -f "$venv_path/bin/activate" ]; then
        if "$venv_path/bin/python" -c "import numpy" 2>/dev/null; then
            echo "Activating venv at $venv_path (numpy OK)"
            set +u
            source "$venv_path/bin/activate"
            set -u
            break
        fi
    fi
done

python3 {restart_script} {spinup_for_restart} {restart_name}
EXIT=$?
if [ $EXIT -eq 0 ]; then
    echo "Job completed at $(date)" > {job_folder}/.completed
fi
exit $EXIT
"""

                    script_path = f"{job_folder}/job.sh"
                    stdin, stdout, stderr = ssh.exec_command(
                        f"cat > {script_path} << 'SCRIPT_EOF'\n{sbatch_script}\nSCRIPT_EOF"
                    )
                    if stdout.channel.recv_exit_status() != 0:
                        err = stderr.read().decode()
                        ssh.close()
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "status": "failed",
                                "error": f"Failed to write script: {err}",
                            }
                        )
                        return

                    ssh.exec_command(f"chmod +x {script_path}")
                    stdout.channel.recv_exit_status()

                    stdin, stdout, stderr = ssh.exec_command(f"sbatch {script_path}")
                    result = stdout.read().decode().strip()
                    error = stderr.read().decode().strip()
                    exit_status = stdout.channel.recv_exit_status()

                    if exit_status == 0 and "Submitted batch job" in result:
                        bouchet_job_id = result.split()[-1]
                        try:
                            stdin2, stdout2, stderr2 = ssh.exec_command(
                                f"echo '{bouchet_job_id}' > {job_folder}/.bouchet_job_id"
                            )
                            stdout2.channel.recv_exit_status()
                        except Exception:
                            pass
                        ssh.close()
                        JOB_STATUS_CACHE[job_id].update(
                            {"bouchet_job_id": bouchet_job_id, "status": "submitted"}
                        )
                        current_app.logger.info(
                            f"SCEPTER run-model job {job_id} submitted with Bouchet ID {bouchet_job_id}"
                        )
                    else:
                        ssh.close()
                        JOB_STATUS_CACHE[job_id].update(
                            {"status": "failed", "error": error or "sbatch failed"}
                        )
                        current_app.logger.error(
                            f"SCEPTER run-model job {job_id} failed: {error}"
                        )
                except Exception as e:
                    import traceback

                    current_app.logger.error(
                        f"SCEPTER run-model submission error for {job_id}: {str(e)}"
                    )
                    current_app.logger.error(traceback.format_exc())
                    JOB_STATUS_CACHE[job_id].update(
                        {"status": "failed", "error": str(e)}
                    )

        threading.Thread(target=submit_run_model_background, daemon=True).start()

        return jsonify(
            {
                "job_id": job_id,
                "bouchet_job_id": None,
                "status": "submitting",
                "message": f"SCEPTER model run is being submitted. Job ID: {job_id}.",
                "parameters": {
                    "spinup_name": spinup_name,
                    "spinup_for_restart": spinup_for_restart,
                    "restart_name": restart_name,
                    "particle_size": params_extras.get("particle_size"),
                    "application_rate": params_extras.get(
                        "application_rate"
                    ),  # g/m²/yr
                    "target_pH": params_extras.get("target_pH"),
                },
            }
        )

    except Exception as e:
        import traceback

        current_app.logger.error(f"Error in submit_run_scepter_model: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@scepter_bp.route("/api/run-scepter-model-batch", methods=["OPTIONS", "POST"])
@scepter_bp.route("/api/scepter/run-model-batch", methods=["OPTIONS", "POST"])
def submit_run_scepter_model_batch():
    """
    Run SCEPTER model for multiple locations. Each location is submitted as a separate SLURM job.

    Payload: {
        "locations": [
            {
                "spinup_name": "...",
                "restart_name": "...",
                "particle_size": <required>,
                "application_rate": <required>,  # ton/ha/yr
                "target_pH": <optional>
            },
            ...
        ]
    }
    """
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        if (
            not os.getenv("BOUCHET_HOST")
            or not os.getenv("BOUCHET_USER")
            or not os.getenv("SSH_PRIVATE_KEY")
        ):
            raise Exception(
                "BOUCHET_HOST, BOUCHET_USER, and SSH_PRIVATE_KEY must be set"
            )

        payload = request.get_json(silent=True) or {}
        locations = (
            payload.get("locations")
            or payload.get("sites")
            or payload.get("location")
        )

        if not locations or not isinstance(locations, list) or len(locations) == 0:
            current_app.logger.warning(
                "run-scepter-model-batch 400: missing or empty locations/sites/location array; "
                "keys=%s",
                list(payload.keys()) if isinstance(payload, dict) else "n/a",
            )
            return (
                jsonify(
                    {
                        "error": "locations array is required with at least one location (spinup_name, restart_name, particle_size, application_rate)",
                        "hint": "Send JSON body key 'locations' (or 'sites') as an array of objects.",
                    }
                ),
                400,
            )

        timestamp = int(time.time())
        batch_id = f"scepter_batch_{str(timestamp)[-6:]}"
        scepter_path = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER"
        restart_script = os.getenv(
            "RESTART_ADD_GBAS_SCRIPT", f"{scepter_path}/restart_add_gbas.py"
        )

        job_ids = []
        job_configs = []

        for i, loc in enumerate(locations):
            spinup_name = (
                loc.get("spinup_name") or loc.get("spinupName") or ""
            ).strip()
            restart_name = (
                loc.get("restart_name") or loc.get("restartName") or ""
            ).strip()
            target_pH = (
                loc.get("target_pH") or loc.get("targetpH") or loc.get("target_pH_arg")
            )
            particle_size = (
                loc.get("particle_size")
                or loc.get("particleSize")
                or loc.get("site_p80")
            )
            application_rate = (
                loc.get("application_rate")
                or loc.get("applicationRate")
                or loc.get("site_app_rate_tha")
            )

            if not spinup_name:
                current_app.logger.warning(
                    "run-scepter-model-batch 400: location %s missing spinup_name", i
                )
                return (
                    jsonify(
                        {
                            "error": f"Location {i}: spinup_name is required",
                            "index": i,
                        }
                    ),
                    400,
                )
            if not restart_name:
                current_app.logger.warning(
                    "run-scepter-model-batch 400: location %s missing restart_name", i
                )
                return (
                    jsonify(
                        {"error": f"Location {i}: restart_name is required", "index": i}
                    ),
                    400,
                )
            if particle_size is None:
                current_app.logger.warning(
                    "run-scepter-model-batch 400: location %s missing particle_size", i
                )
                return (
                    jsonify(
                        {
                            "error": f"Location {i}: particle_size is required",
                            "index": i,
                        }
                    ),
                    400,
                )
            if application_rate is None:
                current_app.logger.warning(
                    "run-scepter-model-batch 400: location %s missing application_rate",
                    i,
                )
                return (
                    jsonify(
                        {
                            "error": f"Location {i}: application_rate is required",
                            "index": i,
                        }
                    ),
                    400,
                )

            application_rate_tha = float(application_rate)
            application_rate_g_m2_yr = application_rate_tha * 100.0

            spinup_batch_id = (
                loc.get("spinup_batch_id")
                or loc.get("baseline_batch_id")
                or loc.get("spinupBatchId")
                or loc.get("baselineBatchId")
            )
            spinup_for_restart = _effective_spinup_for_restart(
                spinup_name, spinup_batch_id=spinup_batch_id
            )

            job_id = f"scepter_run_{str(timestamp)[-5:]}_{i}"
            job_folder = (
                f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}"
            )

            params_extras = {
                "particle_size": particle_size,
                "application_rate": application_rate_g_m2_yr,
            }
            if target_pH is not None and str(target_pH).strip():
                params_extras["target_pH"] = target_pH
            else:
                params_extras["target_pH"] = "(not set)"

            p80str = str(particle_size)
            if p80str.isdigit():
                p80str = f"{p80str}um"
            has_target_pH = target_pH is not None and str(target_pH).strip() != ""
            target_pH_arg = str(target_pH).strip() if has_target_pH else ""

            job_ids.append(job_id)
            job_configs.append(
                {
                    "job_id": job_id,
                    "job_folder": job_folder,
                    "spinup_name": spinup_name,
                    "spinup_for_restart": spinup_for_restart,
                    "restart_name": restart_name,
                    "params_extras": params_extras,
                    "p80str": p80str,
                    "fdust": application_rate_g_m2_yr,
                    "target_pH_arg": target_pH_arg,
                    "restart_script": restart_script,
                    "scepter_path": scepter_path,
                }
            )

            JOB_STATUS_CACHE[job_id] = {
                "job_id": job_id,
                "bouchet_job_id": None,
                "job_folder": job_folder,
                "job_type": "scepter_run",
                "parameters": {
                    "spinup_name": spinup_name,
                    "spinup_for_restart": spinup_for_restart,
                    "restart_name": restart_name,
                    **params_extras,
                },
                "status": "submitting",
                "submitted_at": time.time(),
            }

        BATCH_JOB_CACHE[batch_id] = {
            "batch_id": batch_id,
            "job_ids": job_ids,
            "submitted_at": time.time(),
            "job_type": "scepter_run",
        }
        _persist_scepter_batch_record(batch_id, BATCH_JOB_CACHE[batch_id])

        app = current_app._get_current_object()

        def submit_batch_background():
            with app.app_context():
                ssh = None
                try:
                    ssh = get_ssh_connection()
                    for cfg in job_configs:
                        try:
                            current_app.logger.info(
                                f"Submitting batch job {cfg['job_id']}"
                            )
                            params_data = {
                                "spinup_name": cfg["spinup_name"],
                                "spinup_for_restart": cfg["spinup_for_restart"],
                                "restart_name": cfg["restart_name"],
                                "particle_size": cfg["params_extras"].get(
                                    "particle_size"
                                ),
                                "application_rate": cfg["params_extras"].get(
                                    "application_rate"
                                ),
                                "target_pH": cfg["params_extras"].get(
                                    "target_pH", "(not set)"
                                ),
                            }
                            params_json = json.dumps(params_data, indent=2)
                            job_folder = cfg["job_folder"]
                            commands = [
                                f"mkdir -p {job_folder}",
                                f"echo '{cfg['spinup_for_restart']}' > {job_folder}/spinup_name.txt",
                                f"echo '{cfg['restart_name']}' > {job_folder}/restart_name.txt",
                                f"cat > {job_folder}/parameters.json << 'PARAMS_EOF'\n{params_json}\nPARAMS_EOF",
                            ]
                            for cmd in commands:
                                stdin, stdout, stderr = ssh.exec_command(cmd)
                                if stdout.channel.recv_exit_status() != 0:
                                    err = stderr.read().decode()
                                    JOB_STATUS_CACHE[cfg["job_id"]].update(
                                        {"status": "failed", "error": err}
                                    )
                                    continue

                            scepter_jobs_dir = (
                                f"{cfg['scepter_path'].rstrip('/')}/jobs/"
                            )
                            restart_dir_full = (
                                f"{job_folder.rstrip('/')}/{cfg['restart_name']}"
                            )

                            sbatch_script = f"""#!/bin/bash
#SBATCH --job-name={cfg['job_id'][:12]}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output={job_folder}/%x_%j.out
#SBATCH --error={job_folder}/%x_%j.err

set -x
cd {cfg['scepter_path']}

export SCEPTER_JOBS_DIR={shlex.quote(scepter_jobs_dir)}
export SCEPTER_RESTART_DIR={shlex.quote(restart_dir_full)}

for venv_path in ".venv" "venv" "../venv"; do
    if [ -f "$venv_path/bin/activate" ]; then
        if "$venv_path/bin/python" -c "import numpy" 2>/dev/null; then
            echo "Activating venv at $venv_path (numpy OK)"
            set +u
            source "$venv_path/bin/activate"
            set -u
            break
        fi
    fi
done

python3 {cfg['restart_script']} {cfg['spinup_for_restart']} {cfg['restart_name']} {cfg['p80str']} {cfg['fdust']}{' ' + cfg['target_pH_arg'] if cfg.get('target_pH_arg') else ''}
EXIT=$?
if [ $EXIT -eq 0 ]; then
    echo "Job completed at $(date)" > {job_folder}/.completed
fi
exit $EXIT
"""
                            script_path = f"{job_folder}/job.sh"
                            stdin, stdout, stderr = ssh.exec_command(
                                f"cat > {script_path} << 'SCRIPT_EOF'\n{sbatch_script}\nSCRIPT_EOF"
                            )
                            if stdout.channel.recv_exit_status() != 0:
                                err = stderr.read().decode()
                                JOB_STATUS_CACHE[cfg["job_id"]].update(
                                    {
                                        "status": "failed",
                                        "error": f"Failed to write script: {err}",
                                    }
                                )
                                continue

                            ssh.exec_command(f"chmod +x {script_path}")
                            stdout.channel.recv_exit_status()

                            stdin, stdout, stderr = ssh.exec_command(
                                f"sbatch {script_path}"
                            )
                            result = stdout.read().decode().strip()
                            error = stderr.read().decode().strip()
                            exit_status = stdout.channel.recv_exit_status()

                            if exit_status == 0 and "Submitted batch job" in result:
                                bouchet_job_id = result.split()[-1]
                                try:
                                    ssh.exec_command(
                                        f"echo '{bouchet_job_id}' > {job_folder}/.bouchet_job_id"
                                    )
                                    stdout.channel.recv_exit_status()
                                except Exception:
                                    pass
                                JOB_STATUS_CACHE[cfg["job_id"]].update(
                                    {
                                        "bouchet_job_id": bouchet_job_id,
                                        "status": "submitted",
                                    }
                                )
                                current_app.logger.info(
                                    f"Batch job {cfg['job_id']} submitted with Bouchet ID {bouchet_job_id}"
                                )
                            else:
                                JOB_STATUS_CACHE[cfg["job_id"]].update(
                                    {
                                        "status": "failed",
                                        "error": error or "sbatch failed",
                                    }
                                )
                        except Exception as e:
                            import traceback

                            current_app.logger.error(
                                f"Batch submission error for {cfg['job_id']}: {str(e)}"
                            )
                            current_app.logger.error(traceback.format_exc())
                            JOB_STATUS_CACHE[cfg["job_id"]].update(
                                {"status": "failed", "error": str(e)}
                            )
                except Exception as e:
                    import traceback

                    current_app.logger.error(
                        f"Batch submission error for {batch_id}: {str(e)}"
                    )
                    current_app.logger.error(traceback.format_exc())
                finally:
                    if ssh:
                        try:
                            ssh.close()
                        except Exception:
                            pass

        threading.Thread(target=submit_batch_background, daemon=True).start()

        return jsonify(
            {
                "batch_id": batch_id,
                "job_ids": job_ids,
                "status": "submitting",
                "message": f"Submitting {len(job_ids)} SCEPTER model run(s). Each location is a separate SLURM job.",
                "location_count": len(locations),
            }
        )

    except Exception as e:
        import traceback

        current_app.logger.error(f"Error in submit_run_scepter_model_batch: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@scepter_bp.route(
    "/api/run-scepter-model-batch/<batch_id>/status", methods=["GET", "OPTIONS"]
)
@scepter_bp.route(
    "/api/scepter/run-model-batch/<batch_id>/status", methods=["GET", "OPTIONS"]
)
def check_run_scepter_model_batch_status(batch_id):
    """Check status of a SCEPTER run-model batch (all locations)."""
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        batch_info = BATCH_JOB_CACHE.get(batch_id, {})
        job_ids = batch_info.get("job_ids", [])

        if not batch_id.startswith("scepter_batch_"):
            return (
                jsonify({"error": "Invalid batch_id", "batch_id": batch_id}),
                400,
            )

        if not job_ids:
            return jsonify({"error": "Batch not found", "batch_id": batch_id}), 404

        try:
            ssh = get_ssh_connection_pooled()
        except Exception as e:
            return (
                jsonify(
                    {
                        "batch_id": batch_id,
                        "error": str(e),
                        "message": "Could not refresh batch status (SSH unavailable).",
                    }
                ),
                500,
            )

        jobs_status = []
        status_counts = {}
        for jid in job_ids:
            row = _refresh_run_model_job_live(ssh, jid)
            st = row["status"]
            status_counts[st] = status_counts.get(st, 0) + 1
            jobs_status.append(
                {
                    "job_id": row["job_id"],
                    "status": row["status"],
                    "bouchet_job_id": row.get("bouchet_job_id"),
                    "error": row.get("error"),
                }
            )

        overall = _batch_overall_from_status_counts(status_counts, len(job_ids))

        return jsonify(
            {
                "batch_id": batch_id,
                "overall": overall,
                "jobs": jobs_status,
                "status_counts": status_counts,
                "submitted_at": batch_info.get("submitted_at"),
            }
        )

    except Exception as e:
        current_app.logger.error(
            f"Error in check_run_scepter_model_batch_status for {batch_id}: {str(e)}"
        )
        return jsonify({"batch_id": batch_id, "error": str(e)}), 500


@scepter_bp.route("/api/run-scepter-model/<job_id>/status", methods=["GET", "OPTIONS"])
@scepter_bp.route("/api/scepter/run-model/<job_id>/status", methods=["GET", "OPTIONS"])
def check_run_scepter_model_status(job_id):
    """Check status of a SCEPTER run-model job"""
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        job_info = dict(JOB_STATUS_CACHE.get(job_id, {}))
        if not job_id.startswith("scepter_run_"):
            return jsonify({"error": "Invalid run-model job_id", "job_id": job_id}), 400

        if job_info.get("status") == "failed":
            return (
                jsonify(
                    {
                        "job_id": job_id,
                        "status": "failed",
                        "error": job_info.get("error", "Job failed"),
                    }
                ),
                500,
            )

        job_folder = _resolve_run_model_job_folder(job_id, job_info)
        job_info["job_folder"] = job_folder
        JOB_STATUS_CACHE[job_id] = job_info
        bouchet_job_id = job_info.get("bouchet_job_id")

        try:
            ssh = get_ssh_connection_pooled()
        except Exception as e:
            if bouchet_job_id or job_info:
                cached = job_info.get("status", "submitting")
                return jsonify(
                    {
                        "job_id": job_id,
                        "bouchet_job_id": bouchet_job_id,
                        "status": cached,
                        "message": "Could not verify status (SSH unavailable). Using cached status.",
                        "error": str(e),
                    }
                )
            return (
                jsonify({"job_id": job_id, "status": "unknown", "error": str(e)}),
                500,
            )

        row = _refresh_run_model_job_live(ssh, job_id)
        status = row["status"]
        bouchet_job_id = row.get("bouchet_job_id")
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder",
            _resolve_run_model_job_folder(job_id, job_info),
        )

        logs = []
        if status in ["running", "completed", "failed"]:
            log_cmd = f"tail -n 50 {job_folder}/*.out 2>/dev/null; tail -n 20 {job_folder}/*.err 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()
            if log_content:
                logs = log_content.split("\n")[-15:]

        ssh.close()
        return jsonify(
            {
                "job_id": job_id,
                "bouchet_job_id": bouchet_job_id,
                "status": status,
                "submitted_at": job_info.get("submitted_at"),
                "logs": logs,
            }
        )

    except Exception as e:
        current_app.logger.error(
            f"Error in check_run_scepter_model_status for {job_id}: {str(e)}"
        )
        return jsonify({"job_id": job_id, "status": "unknown", "error": str(e)}), 500


@scepter_bp.route(
    "/api/run-scepter-model/<job_id>/download", methods=["GET", "OPTIONS"]
)
@scepter_bp.route(
    "/api/scepter/run-model/<job_id>/download", methods=["GET", "OPTIONS"]
)
def download_run_scepter_model_results(job_id):
    """Download SCEPTER run-model results as a zip file.

    For ERW runs, outputs are organized under a restart_* folder.
    This endpoint prefers zipping the restart_* folder (derived from restart_name)
    and falls back to the job_folder (scepter_run_*) if needed.
    """
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response, 200

    try:
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder",
            f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}",
        )
        params = job_info.get("parameters", {})
        restart_name = params.get("restart_name")

        # Zip restart outputs: prefer .../scepter_run_*/<restart_name>/ (after post-job mv),
        # else legacy .../jobs/<restart_name>/ beside the run folder.
        jobs_base = os.path.dirname(job_folder.rstrip("/")) or "."
        rn = restart_name.strip() if isinstance(restart_name, str) else ""
        in_job = os.path.join(job_folder, rn) if rn else None
        sibling = os.path.join(jobs_base, rn) if rn else None

        try:
            ssh = get_ssh_connection()
        except Exception as ssh_error:
            error_msg = str(ssh_error)
            current_app.logger.error(
                f"SSH connection failed for run-model download {job_id}: {error_msg}"
            )
            if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
                return (
                    jsonify(
                        {
                            "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                            "job_id": job_id,
                        }
                    ),
                    429,
                )
            return (
                jsonify(
                    {
                        "error": f"Failed to connect to Bouchet HPC: {error_msg}",
                        "job_id": job_id,
                    }
                ),
                500,
            )

        if in_job is not None and sibling is not None:
            pick_cmd = (
                f"if [ -d {shlex.quote(in_job)} ]; then echo in_job; "
                f"elif [ -d {shlex.quote(sibling)} ]; then echo sibling; "
                f"else echo missing; fi"
            )
            stdin, stdout, stderr = ssh.exec_command(pick_cmd)
            pick = (stdout.read().decode() or "").strip()
            if pick == "in_job":
                target_folder_path = in_job
            elif pick == "sibling":
                target_folder_path = sibling
            else:
                target_folder_path = job_folder
        else:
            target_folder_path = job_folder

        # Zip the chosen folder (restart tree or whole scepter_run_* job folder).
        zip_parent = os.path.dirname(target_folder_path.rstrip("/")) or "."
        zip_folder = os.path.basename(target_folder_path.rstrip("/"))
        zip_cmd = f"cd {zip_parent} && zip -r {zip_folder}.zip {zip_folder}/ 2>&1"
        stdin, stdout, stderr = ssh.exec_command(zip_cmd, timeout=120)
        exit_status = stdout.channel.recv_exit_status()
        zip_output = stdout.read().decode()
        error_msg = stderr.read().decode()

        if exit_status != 0:
            current_app.logger.error(
                f"Failed to create run-model zip for {job_id}: {error_msg or zip_output}"
            )
            return (
                jsonify(
                    {
                        "error": f"Failed to create zip on Bouchet: {error_msg or zip_output}",
                        "job_id": job_id,
                    }
                ),
                500,
            )

        # Download the zip file into memory
        sftp = ssh.open_sftp()
        zip_path = f"{zip_parent}/{zip_folder}.zip"
        remote_file = sftp.open(zip_path, "rb")
        zip_data = remote_file.read()
        remote_file.close()
        sftp.close()
        ssh.close()

        response = make_response(zip_data)
        response.headers["Content-Type"] = "application/zip"
        response.headers["Content-Disposition"] = (
            f"attachment; filename={zip_folder}.zip"
        )
        return response

    except Exception as e:
        error_msg = str(e)
        current_app.logger.error(
            f"Error in download_run_scepter_model_results for {job_id}: {error_msg}"
        )
        import traceback

        current_app.logger.error(f"Traceback: {traceback.format_exc()}")

        if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
            return (
                jsonify(
                    {
                        "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                        "job_id": job_id,
                    }
                ),
                429,
            )

        return (
            jsonify({"error": f"Failed to download run-model results: {error_msg}"}),
            500,
        )
