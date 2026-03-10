"""
SCEPTER blueprint: baseline simulation (spinup), run model (restart_add_gbas), status, results, download.
Uses spinup + restart flow (no standalone pipeline).
"""

from flask import Blueprint, request, jsonify, make_response, current_app
import os
import json
import time
import re
import threading

from utils.ssh import get_ssh_connection, get_ssh_connection_pooled, BOUCHET_USER

scepter_bp = Blueprint("scepter", __name__)
JOB_STATUS_CACHE = {}  # job_id -> { job_id, bouchet_job_id, job_folder, status, ... }


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


@scepter_bp.route(
    "/api/baseline-simulation/<job_id>/status", methods=["GET", "OPTIONS"]
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
        job_info = JOB_STATUS_CACHE.get(job_id, {})
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

        bouchet_job_id = job_info.get("bouchet_job_id")
        job_folder = job_info.get(
            "job_folder",
            f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}",
        )

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

        if not bouchet_job_id:
            check = f"test -d {job_folder} && echo exists || echo not_found"
            stdin, stdout, stderr = ssh.exec_command(check)
            if stdout.read().decode().strip() == "not_found":
                # Job is in our cache (submitted from API) but folder not on Bouchet yet
                if job_info and job_info.get("status") == "submitting":
                    return jsonify(
                        {
                            "job_id": job_id,
                            "status": "submitting",
                            "message": "Job is being submitted...",
                        }
                    )
                return jsonify({"error": "Job not found", "job_id": job_id}), 404
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
                    return jsonify(
                        {
                            "job_id": job_id,
                            "status": "completed",
                            "message": "Job completed",
                        }
                    )
                return jsonify(
                    {
                        "job_id": job_id,
                        "status": "submitting",
                        "message": "Job is being submitted...",
                    }
                )

        status_map = {
            "PENDING": "pending",
            "RUNNING": "running",
            "COMPLETED": "completed",
            "FAILED": "failed",
            "CANCELLED": "failed",
            "TIMEOUT": "failed",
            "OUT_OF_MEMORY": "failed",
            "OUT_OF_MEMMORY": "failed",
            "OUT_OF_ME+": "failed",
        }

        squeue_cmd = f"squeue -j {bouchet_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status = (stdout.read().decode().strip().split("\n")[0] or "").strip()

        if slurm_status:
            status = status_map.get(slurm_status, "unknown")
            if status == "unknown" and slurm_status.startswith("OUT_OF_ME"):
                status = "failed"
        else:
            check_done = f"test -f {job_folder}/.completed && echo completed || echo not_completed"
            stdin, stdout, stderr = ssh.exec_command(check_done)
            status = (
                "completed"
                if stdout.read().decode().strip() == "completed"
                else "unknown"
            )
            if status == "unknown":
                sacct_cmd = f"sacct -j {bouchet_job_id} --format=State --noheader --parsable2 2>/dev/null | head -1 | cut -d'|' -f1"
                stdin, stdout, stderr = ssh.exec_command(sacct_cmd)
                sacct_status = stdout.read().decode().strip()
                if sacct_status:
                    status = status_map.get(sacct_status, "unknown")
                    if sacct_status.startswith("OUT_OF_ME"):
                        status = "failed"

        job_info["status"] = status
        JOB_STATUS_CACHE[job_id] = job_info

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
                        f"echo '{spinup_name}' > {job_folder}/spinup_name.txt",
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

python3 {restart_script} {spinup_name} {restart_name}
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
        job_info = JOB_STATUS_CACHE.get(job_id, {})
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

        bouchet_job_id = job_info.get("bouchet_job_id")
        job_folder = job_info.get(
            "job_folder",
            f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}",
        )

        try:
            ssh = get_ssh_connection()
        except Exception as e:
            return (
                jsonify({"job_id": job_id, "status": "unknown", "error": str(e)}),
                500,
            )

        if not bouchet_job_id:
            check = f"test -d {job_folder} && echo exists || echo not_found"
            stdin, stdout, stderr = ssh.exec_command(check)
            if stdout.read().decode().strip() == "not_found":
                ssh.close()
                return jsonify({"error": "Job not found", "job_id": job_id}), 404
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
                    ssh.close()
                    return jsonify(
                        {
                            "job_id": job_id,
                            "status": "completed",
                            "message": "Job completed",
                        }
                    )
                ssh.close()
                return jsonify(
                    {
                        "job_id": job_id,
                        "status": "submitting",
                        "message": "Job is being submitted...",
                    }
                )

        status_map = {
            "PENDING": "pending",
            "RUNNING": "running",
            "COMPLETED": "completed",
            "FAILED": "failed",
            "CANCELLED": "failed",
            "TIMEOUT": "failed",
            "OUT_OF_MEMORY": "failed",
            "OUT_OF_MEMMORY": "failed",
            "OUT_OF_ME+": "failed",
        }

        squeue_cmd = f"squeue -j {bouchet_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status = (stdout.read().decode().strip().split("\n")[0] or "").strip()

        if slurm_status:
            status = status_map.get(slurm_status, "unknown")
            if status == "unknown" and slurm_status.startswith("OUT_OF_ME"):
                status = "failed"
        else:
            check_done = f"test -f {job_folder}/.completed && echo completed || echo not_completed"
            stdin, stdout, stderr = ssh.exec_command(check_done)
            status = (
                "completed"
                if stdout.read().decode().strip() == "completed"
                else "unknown"
            )
            if status == "unknown":
                sacct_cmd = f"sacct -j {bouchet_job_id} --format=State --noheader --parsable2 2>/dev/null | head -1 | cut -d'|' -f1"
                stdin, stdout, stderr = ssh.exec_command(sacct_cmd)
                sacct_status = stdout.read().decode().strip()
                if sacct_status:
                    status = status_map.get(sacct_status, "unknown")
                    if sacct_status.startswith("OUT_OF_ME"):
                        status = "failed"

        job_info["status"] = status
        JOB_STATUS_CACHE[job_id] = job_info

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


