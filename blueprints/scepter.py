"""
SCEPTER blueprint: full-pipeline submit, status, results, download.
Single-coordinate Fortran model (feedstock, particle_size, application_rate, target_soil_ph).
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
# Submit full pipeline
# ---------------------------------------------------------------------------


@scepter_bp.route("/api/run-scepter", methods=["OPTIONS", "POST"])
def run_scepter():
    """Alias for POST /api/scepter/full-pipeline. Same payload and behavior."""
    return submit_full_scepter_pipeline()


@scepter_bp.route("/api/scepter/full-pipeline", methods=["OPTIONS", "POST"])
def submit_full_scepter_pipeline():
    """
    Submit a complete SCEPTER pipeline job.

    Expected payload:
    {
        "coordinate": [lat, lon],  # Single coordinate point
        "feedstock": "basalt" or "olivine",
        "particle_size": 100, 320, 1220, or 3000,  # in µm
        "application_rate": 1.0,  # t/ha/yr
        "target_soil_ph": 7.0  # optional
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
        if not os.getenv("BOUCHET_HOST"):
            raise Exception("BOUCHET_HOST environment variable not set")
        if not os.getenv("BOUCHET_USER"):
            raise Exception("BOUCHET_USER environment variable not set")
        if not os.getenv("SSH_PRIVATE_KEY"):
            raise Exception("SSH_PRIVATE_KEY environment variable not set")

        payload = request.get_json(silent=True) or {}
        current_app.logger.debug(f"SCEPTER full pipeline request payload: {payload}")

        # Extract coordinate (single point)
        coordinate = payload.get("coordinate") or payload.get("coordinates")
        if coordinate and isinstance(coordinate, list) and len(coordinate) == 2:
            coordinate = coordinate
        elif isinstance(coordinate, list) and len(coordinate) > 0:
            coordinate = coordinate[0]
        else:
            return (
                jsonify({"error": "A single coordinate [lat, lon] is required"}),
                400,
            )

        if not isinstance(coordinate, list) or len(coordinate) != 2:
            return (
                jsonify({"error": "Invalid coordinate format. Expected [lat, lon]"}),
                400,
            )
        lat, lon = coordinate[0], coordinate[1]
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            return (
                jsonify({"error": "Latitude and longitude must be numbers"}),
                400,
            )
        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return jsonify({"error": "Coordinates out of valid range"}), 400

        feedstock = payload.get("feedstock", "basalt")
        particle_size = payload.get("particle_size")
        application_rate = payload.get("application_rate")
        target_soil_ph = payload.get("target_soil_ph")

        if feedstock not in ["basalt", "olivine"]:
            return jsonify({"error": "feedstock must be 'basalt' or 'olivine'"}), 400

        if particle_size is None:
            return jsonify({"error": "particle_size is required"}), 400
        particle_size = int(particle_size)
        if particle_size not in [100, 320, 1220, 3000]:
            return (
                jsonify(
                    {"error": "particle_size must be 100, 320, 1220, or 3000 (µm)"}
                ),
                400,
            )

        if application_rate is None:
            return jsonify({"error": "application_rate is required"}), 400
        application_rate = float(application_rate)
        if application_rate <= 0:
            return jsonify({"error": "application_rate must be positive"}), 400

        if target_soil_ph is not None:
            target_soil_ph = float(target_soil_ph)
            if target_soil_ph < 0 or target_soil_ph > 14:
                return (
                    jsonify({"error": "target_soil_ph must be between 0 and 14"}),
                    400,
                )

        timestamp = int(time.time())
        job_id = f"scepter_{str(timestamp)[-5:]}"
        job_folder = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}"
        output_dir = f"{job_folder}/output"
        scepter_path = f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER"

        params_data = {
            "coordinate": coordinate,
            "feedstock": feedstock,
            "particle_size": particle_size,
            "application_rate": application_rate,
        }
        if target_soil_ph is not None:
            params_data["target_soil_ph"] = target_soil_ph

        JOB_STATUS_CACHE[job_id] = {
            "job_id": job_id,
            "bouchet_job_id": None,
            "parameters": params_data,
            "job_folder": job_folder,
            "output_dir": output_dir,
            "status": "submitting",
            "submitted_at": time.time(),
        }

        app = current_app._get_current_object()

        def submit_job_background():
            with app.app_context():
                try:
                    current_app.logger.info(
                        f"Starting background submission for SCEPTER job {job_id}"
                    )
                    ssh = get_ssh_connection()

                    params_json = json.dumps(params_data, indent=2)
                    commands = [
                        f"mkdir -p {job_folder}",
                        f"mkdir -p {output_dir}",
                        f"echo '{params_json}' > {job_folder}/parameters.json",
                    ]
                    for cmd in commands:
                        stdin, stdout, stderr = ssh.exec_command(cmd)
                        exit_status = stdout.channel.recv_exit_status()
                        if exit_status != 0:
                            error_msg = stderr.read().decode()
                            ssh.close()
                            JOB_STATUS_CACHE[job_id].update(
                                {
                                    "status": "failed",
                                    "error": f"Failed to setup job folder: {error_msg}",
                                }
                            )
                            return

                    target_ph_value = (
                        str(target_soil_ph) if target_soil_ph is not None else "NONE"
                    )
                    target_ph_display = (
                        str(target_soil_ph)
                        if target_soil_ph is not None
                        else "Not specified"
                    )

                    # SLURM script: no leading spaces so #SBATCH is at column 1
                    sbatch_script = f"""#!/bin/bash
#SBATCH --job-name={job_id[:12]}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --output={job_folder}/%x_%j.out
#SBATCH --error={job_folder}/%x_%j.err

set -x

if ! cd {scepter_path}; then
    echo "ERROR: Cannot find directory {scepter_path}"
    exit 1
fi

echo "Working directory: $(pwd)"

if module avail GCC 2>&1 | grep -q "GCC/"; then
    module load GCC/12.2.0 2>&1 || module load GCC 2>&1 || echo "Warning: Could not load GCC module"
fi

if ! command -v gfortran &> /dev/null && ! command -v ifort &> /dev/null; then
    echo "ERROR: Fortran compiler (gfortran or ifort) not found"
    exit 1
fi

INPUT_FILE="{job_folder}/scepter_input.txt"
echo "{lat} {lon} {feedstock} {particle_size} {application_rate} {target_ph_value}" > $INPUT_FILE

echo "=========================================="
echo "SCEPTER Model Run"
echo "=========================================="
echo "Input parameters:"
echo "  Coordinate: [{lat}, {lon}]"
echo "  Feedstock: {feedstock}"
echo "  Particle Size: {particle_size} µm"
echo "  Application Rate: {application_rate} t/ha/yr"
echo "  Target Soil pH: {target_ph_display}"
echo "=========================================="

SCEPTER_EXEC="scepter"
if [ -f "$SCEPTER_EXEC" ]; then
    ./$SCEPTER_EXEC < $INPUT_FILE > {output_dir}/scepter_output.txt 2>&1
    EXIT_CODE=$?
elif [ -f "bin/$SCEPTER_EXEC" ]; then
    ./bin/$SCEPTER_EXEC < $INPUT_FILE > {output_dir}/scepter_output.txt 2>&1
    EXIT_CODE=$?
elif [ -f "build/$SCEPTER_EXEC" ]; then
    ./build/$SCEPTER_EXEC < $INPUT_FILE > {output_dir}/scepter_output.txt 2>&1
    EXIT_CODE=$?
else
    echo "ERROR: SCEPTER executable not found."
    exit 1
fi

if [ $EXIT_CODE -ne 0 ]; then
    echo "ERROR: SCEPTER model run failed with exit code $EXIT_CODE"
    exit 1
fi

if [ -d "output" ]; then
    cp -r output/* {output_dir}/ 2>/dev/null || true
fi

echo "Job completed at $(date)" > {job_folder}/.completed
echo "=========================================="
echo "SCEPTER pipeline completed successfully!"
echo "Results saved to: {output_dir}"
echo "=========================================="
"""

                    script_path = f"{job_folder}/job.sh"
                    stdin, stdout, stderr = ssh.exec_command(
                        f"cat > {script_path} << 'SCRIPT_EOF'\n{sbatch_script}\nSCRIPT_EOF"
                    )
                    exit_status = stdout.channel.recv_exit_status()
                    if exit_status != 0:
                        error_msg = stderr.read().decode()
                        ssh.close()
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "status": "failed",
                                "error": f"Failed to write job script: {error_msg}",
                            }
                        )
                        return

                    stdin, stdout, stderr = ssh.exec_command(f"chmod +x {script_path}")
                    exit_status = stdout.channel.recv_exit_status()

                    sbatch_cmd = f"sbatch {script_path}"
                    stdin, stdout, stderr = ssh.exec_command(sbatch_cmd)
                    result = stdout.read().decode().strip()
                    error = stderr.read().decode().strip()
                    exit_status = stdout.channel.recv_exit_status()

                    if exit_status == 0 and "Submitted batch job" in result:
                        bouchet_job_id = result.split()[-1]
                        try:
                            save_cmd = f"echo '{bouchet_job_id}' > {job_folder}/.bouchet_job_id"
                            stdin2, stdout2, stderr2 = ssh.exec_command(save_cmd)
                            stdout2.channel.recv_exit_status()
                        except Exception:
                            pass
                        ssh.close()
                        JOB_STATUS_CACHE[job_id].update(
                            {"bouchet_job_id": bouchet_job_id, "status": "submitted"}
                        )
                        current_app.logger.info(
                            f"SCEPTER job {job_id} submitted successfully with Bouchet job ID {bouchet_job_id}"
                        )
                    else:
                        ssh.close()
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "status": "failed",
                                "error": f"Failed to submit job: {error}",
                            }
                        )
                        current_app.logger.error(
                            f"SCEPTER job {job_id} submission failed: {error}"
                        )
                except Exception as e:
                    import traceback

                    current_app.logger.error(
                        f"Error in background submission for SCEPTER job {job_id}: {str(e)}"
                    )
                    current_app.logger.error(traceback.format_exc())
                    JOB_STATUS_CACHE[job_id].update(
                        {"status": "failed", "error": str(e)}
                    )

        thread = threading.Thread(target=submit_job_background, daemon=True)
        thread.start()

        return jsonify(
            {
                "job_id": job_id,
                "bouchet_job_id": None,
                "status": "submitting",
                "message": f"SCEPTER job is being submitted.\n\n Job ID: {job_id}.",
                "parameters": params_data,
            }
        )

    except Exception as e:
        import traceback

        current_app.logger.error(f"Error in submit_full_scepter_pipeline: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return (
            jsonify(
                {
                    "error": f"Backend error: {str(e)}",
                    "details": traceback.format_exc() if current_app.debug else None,
                }
            ),
            500,
        )


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
python3 {create_spinup_script} {job_folder}
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
    Payload: { "spinup_name": "...", "restart_name": "..." }
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

        spinup_name = spinup_name.strip()
        restart_name = restart_name.strip()

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
            "parameters": {"spinup_name": spinup_name, "restart_name": restart_name},
            "status": "submitting",
            "submitted_at": time.time(),
        }

        app = current_app._get_current_object()

        def submit_run_model_background():
            with app.app_context():
                try:
                    current_app.logger.info(f"Starting SCEPTER run-model job {job_id}")
                    ssh = get_ssh_connection()

                    commands = [
                        f"mkdir -p {job_folder}",
                        f"echo '{spinup_name}' > {job_folder}/spinup_name.txt",
                        f"echo '{restart_name}' > {job_folder}/restart_name.txt",
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
                },
            }
        )

    except Exception as e:
        import traceback

        current_app.logger.error(f"Error in submit_run_scepter_model: {str(e)}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


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


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


@scepter_bp.route(
    "/api/scepter/full-pipeline/<job_id>/status", methods=["GET", "OPTIONS"]
)
def check_full_scepter_pipeline_status(job_id):
    """Check the status of a full SCEPTER pipeline job"""
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
        if job_info.get("status") == "failed":
            return (
                jsonify(
                    {
                        "job_id": job_id,
                        "status": "failed",
                        "error": job_info.get("error", "Job submission failed"),
                    }
                ),
                500,
            )

        bouchet_job_id = job_info.get("bouchet_job_id")
        job_folder = job_info.get(
            "job_folder",
            f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}",
        )
        submitted_at = job_info.get("submitted_at", time.time())
        time_elapsed = time.time() - submitted_at

        try:
            ssh = get_ssh_connection()
        except Exception as ssh_error:
            current_app.logger.error(
                f"SSH connection failed for Bouchet job {job_id}: {str(ssh_error)}"
            )
            if time_elapsed < 60:
                return jsonify(
                    {
                        "job_id": job_id,
                        "status": "submitting",
                        "message": "Job is being submitted to Bouchet HPC. Please wait...",
                    }
                )
            return (
                jsonify(
                    {
                        "job_id": job_id,
                        "status": "unknown",
                        "error": str(ssh_error),
                    }
                ),
                500,
            )

        # Recover bouchet_job_id if missing
        if not bouchet_job_id:
            check_folder = f"test -d {job_folder} && echo 'exists' || echo 'not_found'"
            stdin, stdout, stderr = ssh.exec_command(check_folder)
            if stdout.read().decode().strip() == "not_found":
                ssh.close()
                if time_elapsed < 300:
                    return jsonify(
                        {
                            "job_id": job_id,
                            "bouchet_job_id": None,
                            "status": "submitting",
                            "message": "Job is being submitted to Bouchet HPC. Please wait...",
                        }
                    )
                return jsonify({"error": "Job not found.", "job_id": job_id}), 404

            read_cmd = f"cat {job_folder}/.bouchet_job_id 2>/dev/null || (grep -h 'Submitted batch job' {job_folder}/*.out 2>/dev/null | tail -1 | awk '{{print $NF}}')"
            stdin, stdout, stderr = ssh.exec_command(read_cmd)
            recovered = stdout.read().decode().strip()
            if recovered:
                bouchet_job_id = recovered
                job_info["bouchet_job_id"] = bouchet_job_id
                JOB_STATUS_CACHE[job_id] = job_info
            else:
                check_completion = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
                stdin, stdout, stderr = ssh.exec_command(check_completion)
                if stdout.read().decode().strip() == "completed":
                    ssh.close()
                    return jsonify(
                        {
                            "job_id": job_id,
                            "status": "completed",
                            "message": "Job completed (recovered from Bouchet filesystem)",
                        }
                    )
                ssh.close()
                if time_elapsed < 300:
                    return jsonify(
                        {
                            "job_id": job_id,
                            "bouchet_job_id": None,
                            "status": "submitting",
                            "message": "Job is being submitted to Bouchet HPC. Please wait...",
                        }
                    )
                return (
                    jsonify(
                        {
                            "error": "Job found on Bouchet but status cannot be determined.",
                            "job_id": job_id,
                        }
                    ),
                    500,
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
        slurm_status_raw = stdout.read().decode().strip()
        slurm_status = slurm_status_raw.split("\n")[0] if slurm_status_raw else ""

        if slurm_status:
            status = status_map.get(slurm_status, "unknown")
            if status == "unknown" and slurm_status.startswith("OUT_OF_ME"):
                status = "failed"
        else:
            check_completion = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
            stdin, stdout, stderr = ssh.exec_command(check_completion)
            completion = stdout.read().decode().strip()
            if completion == "completed":
                sacct_cmd = f"sacct -j {bouchet_job_id} --format=State --noheader --parsable2 2>/dev/null | head -1 | cut -d'|' -f1"
                stdin2, stdout2, stderr2 = ssh.exec_command(sacct_cmd)
                sacct_status = stdout2.read().decode().strip()
                if sacct_status:
                    status = status_map.get(sacct_status, "completed")
                    if sacct_status.startswith("OUT_OF_ME"):
                        status = "failed"
                else:
                    status = "completed"
            else:
                if job_info.get("status") == "submitted":
                    sacct_cmd = f"sacct -j {bouchet_job_id} --format=State --noheader --parsable2 2>/dev/null | head -1 | cut -d'|' -f1"
                    stdin2, stdout2, stderr2 = ssh.exec_command(sacct_cmd)
                    sacct_status = stdout2.read().decode().strip()
                    status = (
                        status_map.get(sacct_status, "submitted")
                        if sacct_status
                        else "submitted"
                    )
                    if sacct_status and sacct_status.startswith("OUT_OF_ME"):
                        status = "failed"
                else:
                    status = "unknown"

        # OOM / error detection in logs
        logs = []
        error_message = None
        if status in ["running", "completed", "failed"]:
            log_cmd = f"tail -n 100 {job_folder}/*.out 2>/dev/null; tail -n 50 {job_folder}/*.err 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()
            if log_content:
                logs = log_content.split("\n")[-20:]
            oom_indicators = [
                "out of memory",
                "out-of-memory",
                "outofmemory",
                "memoryerror",
                "killed",
                "cannot allocate memory",
                "memory fault",
                "oom",
                "oom-kill",
            ]
            log_lower = log_content.lower() if log_content else ""
            if any(ind in log_lower for ind in oom_indicators):
                if status == "completed":
                    status = "failed"
                error_message = "Job ran out of memory (OOM) on Bouchet HPC. Try reducing the workload or request more memory."

        job_info["status"] = status
        JOB_STATUS_CACHE[job_id] = job_info
        ssh.close()

        return jsonify(
            {
                "job_id": job_id,
                "bouchet_job_id": bouchet_job_id,
                "status": status,
                "submitted_at": job_info.get("submitted_at"),
                "logs": logs,
                "error_message": error_message,
            }
        )

    except Exception as e:
        current_app.logger.error(
            f"Error in check_full_scepter_pipeline_status for {job_id}: {str(e)}"
        )
        return (
            jsonify({"job_id": job_id, "status": "unknown", "error": str(e)}),
            500,
        )


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@scepter_bp.route(
    "/api/scepter/full-pipeline/<job_id>/results", methods=["GET", "OPTIONS"]
)
def get_full_scepter_pipeline_results(job_id):
    """Get the results of a completed full SCEPTER pipeline job"""
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
        job_folder = job_info.get(
            "job_folder",
            f"/home/{BOUCHET_USER}/project_pi_par35/yhs5/SCEPTER/jobs/{job_id}",
        )
        output_dir = job_info.get("output_dir", f"{job_folder}/output")

        ssh = get_ssh_connection()

        check_cmd = (
            f"test -f {job_folder}/.completed && echo 'completed' || echo 'running'"
        )
        stdin, stdout, stderr = ssh.exec_command(check_cmd)
        job_status = stdout.read().decode().strip()

        list_cmd = f"find {output_dir} -type f 2>/dev/null | head -50"
        stdin, stdout, stderr = ssh.exec_command(list_cmd)
        output_files = [f for f in stdout.read().decode().strip().split("\n") if f]

        tree_cmd = f"ls -la {output_dir} 2>/dev/null || echo ''"
        stdin, stdout, stderr = ssh.exec_command(tree_cmd)
        directory_listing = stdout.read().decode().strip()

        results = {
            "status": job_status,
            "output_directory": output_dir,
            "files": output_files,
            "file_count": len(output_files),
        }
        if job_status == "completed":
            results["message"] = (
                "Job completed. Use download endpoint to retrieve all files."
            )
        else:
            results["message"] = (
                "Job is still running. Partial results may be available."
            )

        ssh.close()
        return jsonify({"job_id": job_id, "status": job_status, "results": results})

    except Exception as e:
        current_app.logger.error(
            f"Error in get_full_scepter_pipeline_results for {job_id}: {str(e)}"
        )
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


@scepter_bp.route(
    "/api/scepter/full-pipeline/<job_id>/download", methods=["GET", "OPTIONS"]
)
def download_full_scepter_pipeline_results(job_id):
    """Download results as a zip file"""
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
        output_dir = job_info.get("output_dir", f"{job_folder}/output")

        ssh = get_ssh_connection()
        zip_cmd = f"cd {job_folder} && zip -r results.zip output/ 2>&1"
        stdin, stdout, stderr = ssh.exec_command(zip_cmd)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            error_msg = stderr.read().decode()
            ssh.close()
            return jsonify({"error": f"Failed to create zip: {error_msg}"}), 500

        sftp = ssh.open_sftp()
        zip_path = f"{job_folder}/results.zip"
        remote_file = sftp.open(zip_path, "rb")
        zip_data = remote_file.read()
        remote_file.close()
        sftp.close()
        ssh.close()

        response = make_response(zip_data)
        response.headers["Content-Type"] = "application/zip"
        response.headers["Content-Disposition"] = (
            f"attachment; filename=scepter_results_{job_id}.zip"
        )
        return response

    except Exception as e:
        current_app.logger.error(
            f"Error in download_full_scepter_pipeline_results for {job_id}: {str(e)}"
        )
        return jsonify({"error": str(e)}), 500
