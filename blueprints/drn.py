"""
DRN blueprint: run-job, check-job-status, full-pipeline, watershed, outlet compatibility.
"""

from flask import Blueprint, request, jsonify, make_response, current_app
from flask_cors import cross_origin
import subprocess
import os
import json
import time
import re
import threading

from utils.ssh import get_ssh_connection, get_ssh_connection_pooled, GRACE_USER

drn_bp = Blueprint("drn", __name__)
JOB_STATUS_CACHE = {}  # In production, use Redis or database

# Project root (parent of blueprints/) for finding scripts like 01_site_selection.py
_DRN_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@drn_bp.route("/api/run-job", methods=["POST"])
def run_job():
    current_app.logger.debug("Received request to run job")
    try:
        # Accept both legacy shape { model, parameters, user_id }
        # and new shape { data: { model_type, locations, numStart, yearRun, timeStep, ... }, user_id? }
        payload = request.get_json(silent=True) or {}
        data_block = (
            payload.get("data") if isinstance(payload.get("data"), dict) else None
        )

        # Model
        model_raw = None
        if data_block:
            model_raw = data_block.get("model_type") or payload.get("model")
        else:
            model_raw = payload.get("model") or payload.get("model_type")
        model = (model_raw or "drn").lower()

        # Parameters
        parameters = None
        if "parameters" in payload and isinstance(payload.get("parameters"), dict):
            parameters = payload.get("parameters")
        elif data_block:
            # Build parameters dict from new shape
            parameters = {
                "locations": data_block.get("locations", []),
                "numStart": data_block.get("numStart"),
                "yearRun": data_block.get("yearRun"),
                "timeStep": data_block.get("timeStep"),
            }
            # Optional arrays/fields (pass-through if provided)
            if "riverInputRates" in data_block:
                parameters["riverInputRates"] = data_block.get("riverInputRates")
            if "extra" in data_block and isinstance(data_block["extra"], dict):
                parameters["extra"] = data_block["extra"]

            # Normalize per-location key variants (e.g., "ewRiverinput rate" -> "ewRiverInputRate")
            if isinstance(parameters.get("locations"), list):
                normalized_locations = []
                for loc in parameters["locations"]:
                    if isinstance(loc, dict):
                        new_loc = dict(loc)
                        # Handle common typos/variants
                        for k in list(new_loc.keys()):
                            if k.strip().lower() in [
                                "ewriverinput rate",
                                "ewriverinputrate",
                                "ew_river_input_rate",
                            ]:
                                new_loc["ewRiverInputRate"] = new_loc.get(k)
                                del new_loc[k]
                            if k.strip() == "ewRiverInput":
                                new_loc["ewRiverInputRate"] = new_loc.get(k)
                                del new_loc[k]
                        normalized_locations.append(new_loc)
                    else:
                        normalized_locations.append(loc)
                parameters["locations"] = normalized_locations
        elif any(
            key in payload for key in ["locations", "numStart", "yearRun", "timeStep"]
        ):
            # Build parameters from flat root-level shape
            parameters = {
                "locations": payload.get("locations", []),
                "numStart": payload.get("numStart"),
                "yearRun": payload.get("yearRun"),
                "timeStep": payload.get("timeStep"),
            }
            # Normalize per-location key variants
            if isinstance(parameters.get("locations"), list):
                normalized_locations = []
                for loc in parameters["locations"]:
                    if isinstance(loc, dict):
                        new_loc = dict(loc)
                        for k in list(new_loc.keys()):
                            if k.strip().lower() in [
                                "ewriverinput rate",
                                "ewriverinputrate",
                                "ew_river_input_rate",
                            ]:
                                new_loc["ewRiverInputRate"] = new_loc.get(k)
                                del new_loc[k]
                            if k.strip() == "ewRiverInput":
                                new_loc["ewRiverInputRate"] = new_loc.get(k)
                                del new_loc[k]
                        normalized_locations.append(new_loc)
                    else:
                        normalized_locations.append(loc)
                parameters["locations"] = normalized_locations
        else:
            parameters = None

        # User id
        user_id = (
            payload.get("user_id")
            or (data_block.get("user_id") if data_block else None)
            or "anonymous"
        )
        user_id_clean = re.sub(r"\W+", "", user_id)

        # Basic validation
        if model not in ["drn"]:
            return (
                jsonify({"error": "Only DRN model supported currently"}),
                400,
            )
        if not isinstance(parameters, dict):
            return jsonify({"error": "Missing or invalid parameters"}), 400

        # Create unique job folder on Grace server (via SSH)
        timestamp = int(time.time())
        project_path = "DRN"
        job_folder = f"/home/{GRACE_USER}/project/{project_path}/tmp_{model}_job_{user_id_clean}_{timestamp}"

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Create job folder and write parameters on Grace
        commands = [
            f"mkdir -p {job_folder}",
            f"echo '{json.dumps(parameters)}' > {job_folder}/parameters.json",
        ]

        for cmd in commands:
            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error_msg = stderr.read().decode()
                ssh.close()
                return (
                    jsonify({"error": f"Failed to setup job folder: {error_msg}"}),
                    500,
                )

        # Submit to SLURM using sbatch via SSH
        project_path = "DRN"
        script_name = "run_drn_job.sh"
        sbatch_cmd = f"sbatch /home/{GRACE_USER}/project/{project_path}/{script_name} {job_folder}"
        stdin, stdout, stderr = ssh.exec_command(sbatch_cmd)

        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()

        ssh.close()

        if exit_status == 0 and "Submitted batch job" in result:
            # Extract job ID from sbatch output
            job_id = result.split()[-1]

            # Cache job info
            JOB_STATUS_CACHE[job_id] = {
                "model": model,
                "parameters": parameters,
                "user_id": user_id,
                "job_folder": job_folder,
                "status": "submitted",
            }

            model_name = model.upper()
            return jsonify(
                {
                    "job_id": job_id,
                    "status": "submitted",
                    "message": f"{model_name} job submitted successfully! Job ID: {job_id}",
                    "sbatch_output": result,
                }
            )
        else:
            return (
                jsonify(
                    {"error": f"Failed to submit job: {error}", "sbatch_output": result}
                ),
                500,
            )

    except Exception as e:
        return jsonify({"error": f"Backend error: {str(e)}"}), 500


@drn_bp.route("/api/check-job-status/<job_id>", methods=["GET"])
def check_job_status(job_id):
    try:
        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Check SLURM job status via SSH
        squeue_cmd = f"squeue -j {job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status_raw = stdout.read().decode().strip()
        slurm_status = slurm_status_raw.split("\n")[0]  # Take only first line
        current_app.logger.debug(f"SLURM status for {job_id}: {slurm_status}")

        if slurm_status:
            # Map SLURM status to our status (OUT_OF_MEMORY -> failed)
            status_map = {
                "PENDING": "pending",
                "RUNNING": "running",
                "COMPLETED": "completed",
                "FAILED": "failed",
                "CANCELLED": "failed",
                "TIMEOUT": "failed",
                "OUT_OF_MEMORY": "failed",
                "OUT_OF_MEMMORY": "failed",
                "OUT_OF_ME+": "failed",  # OUT_OF_MEMORY truncated by sacct
            }
            status = status_map.get(slurm_status, "unknown")
        else:
            # Job not in queue, check if output file exists to determine completion
            check_cmd = f"test -f /home/{GRACE_USER}/project/DRN/tmp_output/drn_{job_id}_1.out && echo 'completed' || echo 'failed'"
            stdin, stdout, stderr = ssh.exec_command(check_cmd)
            output_check = stdout.read().decode().strip()
            status = output_check  # Will be either 'completed' or 'failed'
            current_app.logger.debug(f"Status for {job_id}: {status}")
        # Get job logs if available
        logs = []
        if status in ["completed", "failed"]:
            # Try to get job output
            log_cmd = f"cat /home/{GRACE_USER}/project/DRN/tmp_output/drn_{job_id}_1.out 2>/dev/null || echo 'No output file found'"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()
            logs = log_content.split("\n") if log_content else ["No logs available"]
            current_app.logger.debug(f"Logs for {job_id}: {logs}")
        ssh.close()

        return jsonify(
            {
                "job_id": job_id,
                "status": status,
                "logs": logs,
                "slurm_status": slurm_status,
                "error": None,
            }
        )

    except Exception as e:
        current_app.logger.error(f"Error in check_job_status for {job_id}: {str(e)}")
        return (
            jsonify(
                {
                    "job_id": job_id,
                    "status": "unknown",
                    "error": f"Status check failed: {str(e)}",
                }
            ),
            500,
        )


# Site selection endpoints removed - now handled by full pipeline endpoint


@drn_bp.route("/api/drn/full-pipeline", methods=["OPTIONS", "POST"])
@cross_origin()
def submit_full_drn_pipeline():
    """
    Submit a complete DRN pipeline job (Steps 1-5).

    Expected payload:
    {
        "coordinates": [[lat1, lon1], [lat2, lon2], ...],
        "rate_rock": 1.0,
        "month_run": 12,  # Number of months to run (e.g., 12 = 1 year)
        "time_step": 1.0,
        "feedstock": "carbonate",
        "monte_count": 0
    }
    """
    if request.method == "OPTIONS":
        origin = request.headers.get("Origin")
        if origin and (
            origin in current_app.config.get("CORS_ORIGINS", [])
            or "localhost:5173" in origin
        ):
            response = make_response()
            response.headers.add("Access-Control-Allow-Origin", origin)
            response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
            response.headers.add(
                "Access-Control-Allow-Headers",
                "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
            )
            response.headers.add("Access-Control-Allow-Credentials", "true")
            return response, 200

    try:
        # Validate environment variables
        if not os.getenv("GRACE_HOST"):
            raise Exception("GRACE_HOST environment variable not set")
        if not os.getenv("GRACE_USER"):
            raise Exception("GRACE_USER environment variable not set")
        if not os.getenv("SSH_PRIVATE_KEY"):
            raise Exception("SSH_PRIVATE_KEY environment variable not set")

        payload = request.get_json(silent=True) or {}
        current_app.logger.debug(f"Full pipeline request payload: {payload}")

        # Extract parameters
        coordinates = payload.get("coordinates", [])
        rate_rock = float(payload.get("rate_rock", 1.0))
        month_run = float(payload.get("month_run", 12))  # Default to 12 months (1 year)
        time_step = float(payload.get("time_step", 1.0))
        feedstock = payload.get("feedstock", "carbonate")
        monte_count = int(payload.get("monte_count", 0))

        # Convert months to years for the scripts (12 months = 1 year)
        year_run = month_run / 12.0

        # Validate coordinates
        if not coordinates or not isinstance(coordinates, list):
            return (
                jsonify({"error": "At least 1 coordinate pair is required"}),
                400,
            )
        if len(coordinates) < 1:
            return (
                jsonify({"error": "At least 1 coordinate pair is required"}),
                400,
            )

        for coord in coordinates:
            if not isinstance(coord, list) or len(coord) != 2:
                return (
                    jsonify(
                        {"error": "Invalid coordinate format. Expected [lat, lon]"}
                    ),
                    400,
                )
            lat, lon = coord[0], coord[1]
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                return (
                    jsonify({"error": "Latitude and longitude must be numbers"}),
                    400,
                )
            if lat < -90 or lat > 90 or lon < -180 or lon > 180:
                return jsonify({"error": "Coordinates out of valid range"}), 400

        # Validate other parameters
        if rate_rock <= 0:
            return jsonify({"error": "rate_rock must be positive"}), 400
        if month_run <= 0:
            return jsonify({"error": "month_run must be positive"}), 400
        if time_step <= 0:
            return jsonify({"error": "time_step must be positive"}), 400
        if feedstock not in ["carbonate", "basalt"]:
            return jsonify({"error": "feedstock must be 'carbonate' or 'basalt'"}), 400

        # Generate unique job ID immediately
        timestamp = int(time.time())
        job_id = f"drn_{str(timestamp)[-5:]}"
        job_folder = f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        output_dir = f"{job_folder}/output"
        drn_path = f"/home/{GRACE_USER}/project/DRN"

        # Prepare parameters data
        params_data = {
            "coordinates": coordinates,
            "rate_rock": rate_rock,
            "month_run": month_run,
            "year_run": year_run,  # Keep for internal use (converted from months)
            "time_step": time_step,
            "feedstock": feedstock,
            "monte_count": monte_count,
        }

        # Cache job info immediately with "submitting" status
        JOB_STATUS_CACHE[job_id] = {
            "job_id": job_id,
            "grace_job_id": None,  # Will be set after submission
            "parameters": params_data,
            "job_folder": job_folder,
            "output_dir": output_dir,
            "status": "submitting",  # Initial status
            "submitted_at": time.time(),
        }

        # Start background thread to submit job
        def submit_job_background():
            """Submit job to Grace HPC in background"""
            from app import app

            with app.app_context():
                try:
                    current_app.logger.info(
                        f"Starting background submission for job {job_id}"
                    )

                    # Connect to Grace via SSH
                    ssh = get_ssh_connection()

                    # Create job folder and write parameters
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
                            # Update cache with error
                            JOB_STATUS_CACHE[job_id].update(
                                {
                                    "status": "failed",
                                    "error": f"Failed to setup job folder: {error_msg}",
                                }
                            )
                            return

                    # Create SLURM job script for full pipeline
                    # Use 32G to improve schedulability when Grace is busy (was 64G)
                    sbatch_script = f"""#!/bin/bash
    #SBATCH --job-name={job_id[:12]}
    #SBATCH --ntasks=1
    #SBATCH --cpus-per-task=8
    #SBATCH --mem=32G
    #SBATCH --time=24:00:00
    #SBATCH --output={job_folder}/%x_%j.out
    #SBATCH --error={job_folder}/%x_%j.err
    
    
    set -x  # Print commands for debugging
    
    # Load modules if available
    if module avail Python 2>&1 | grep -q "Python/"; then
        module load Python/3.11 2>&1 || module load Python 2>&1 || echo "Warning: Could not load Python module"
    fi
    
    if module avail GDAL 2>&1 | grep -q "GDAL/"; then
        module load GDAL/3.9.0 2>&1 || module load GDAL 2>&1 || echo "Warning: Could not load GDAL module"
    fi
    
    # Navigate to Python script directory
    if ! cd {drn_path}/R_code/python_version; then
        echo "ERROR: Cannot find directory {drn_path}/R_code/python_version"
        exit 1
    fi
    
    # Find Python interpreter
    PYTHON_CMD=""
    VENV_FOUND=false
    
    for venv_path in ".venv" "venv" "../venv" "../../venv"; do
        if [ -f "$venv_path/bin/python" ]; then
            echo "Found virtual environment at: $venv_path"
            PYTHON_CMD="$venv_path/bin/python"
            VENV_FOUND=true
            break
        fi
    done
    
    if [ "$VENV_FOUND" = false ]; then
        echo "Warning: Virtual environment not found, trying system Python"
        if command -v python3 &> /dev/null; then
            if python3 -c "import geopandas, scipy" 2>/dev/null; then
                PYTHON_CMD="python3"
            else
                echo "ERROR: Cannot find Python with required packages"
                exit 1
            fi
        else
            echo "ERROR: python3 not found"
            exit 1
        fi
    fi
    
    echo "Python environment ready: $($PYTHON_CMD --version)"
    echo "Working directory: $(pwd)"
    
    # Prepare coordinates for Step 1
    COORDS_JSON='{json.dumps([{"lat": c[0], "lon": c[1]} for c in coordinates])}'
    echo "$COORDS_JSON" > {job_folder}/coords.json
    
    # Step 1: Site selection
    echo "=========================================="
    echo "Step 1: Site Selection"
    echo "=========================================="
    $PYTHON_CMD 01_site_selection.py \\
        --coords-file {job_folder}/coords.json \\
        --output-dir {output_dir}
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Step 1 (Site Selection) failed"
        exit 1
    fi
    
    # Step 2: Sample interpolation
    echo "=========================================="
    echo "Step 2: Sample Interpolation"
    echo "=========================================="
    $PYTHON_CMD 02_sample_interpolation.py \\
        --monte-count {monte_count} \\
        --output-dir {output_dir}
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Step 2 (Sample Interpolation) failed"
        exit 1
    fi
    
    # Step 3: DRN preparation
    echo "=========================================="
    echo "Step 3: DRN Preparation"
    echo "=========================================="
    $PYTHON_CMD 03_DRN_prep.py \\
        --feedstock {feedstock} \\
        --rate-rock {rate_rock} \\
        --output-dir {output_dir}
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Step 3 (DRN Preparation) failed"
        exit 1
    fi
    
    # Step 4: DRN run
    echo "=========================================="
    echo "Step 4: DRN Run"
    echo "=========================================="
    $PYTHON_CMD 04_DRN_run.py \\
        --monte-count {monte_count} \\
        --year-run {year_run} \\
        --time-step {time_step} \\
        --output-dir {output_dir}
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Step 4 (DRN Run) failed"
        exit 1
    fi
    
    # Step 5: Compile results
    echo "=========================================="
    echo "Step 5: Compile Results"
    echo "=========================================="
    $PYTHON_CMD 05_after_DRN_compile.py \\
        --monte-count {monte_count} \\
        --year-run {year_run} \\
        --time-step {time_step} \\
        --output-dir {output_dir}
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Step 5 (Compile Results) failed"
        exit 1
    fi
    
    # Create completion marker
    echo "Job completed at $(date)" > {job_folder}/.completed
    echo "=========================================="
    echo "All steps completed successfully!"
    echo "Results saved to: {output_dir}"
    echo "=========================================="
    """
                    # SLURM requires #SBATCH at column 1; strip Python indentation
                    sbatch_script = sbatch_script.replace("\n    ", "\n")

                    # Write SLURM script to Grace
                    script_path = f"{job_folder}/job.sh"
                    stdin, stdout, stderr = ssh.exec_command(
                        f"cat > {script_path} << 'SCRIPT_EOF'\n{sbatch_script}\nSCRIPT_EOF"
                    )
                    exit_status = stdout.channel.recv_exit_status()
                    if exit_status != 0:
                        error_msg = stderr.read().decode()
                        ssh.close()
                        # Update cache with error
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "status": "failed",
                                "error": f"Failed to write job script: {error_msg}",
                            }
                        )
                        return

                    # Make script executable
                    stdin, stdout, stderr = ssh.exec_command(f"chmod +x {script_path}")
                    exit_status = stdout.channel.recv_exit_status()

                    # Submit SLURM job
                    sbatch_cmd = f"sbatch {script_path}"
                    stdin, stdout, stderr = ssh.exec_command(sbatch_cmd)
                    result = stdout.read().decode().strip()
                    error = stderr.read().decode().strip()
                    exit_status = stdout.channel.recv_exit_status()

                    if exit_status == 0 and "Submitted batch job" in result:
                        # Extract Grace job ID from sbatch output
                        grace_job_id = result.split()[-1]

                        # Save grace_job_id to a file in the job folder for persistence
                        # This ensures we can recover it even if the server restarts
                        try:
                            save_job_id_cmd = (
                                f"echo '{grace_job_id}' > {job_folder}/.grace_job_id"
                            )
                            stdin2, stdout2, stderr2 = ssh.exec_command(save_job_id_cmd)
                            exit_status2 = stdout2.channel.recv_exit_status()
                            if exit_status2 == 0:
                                current_app.logger.info(
                                    f"Saved grace_job_id {grace_job_id} to {job_folder}/.grace_job_id"
                                )
                            else:
                                current_app.logger.warning(
                                    f"Failed to save grace_job_id to file: {stderr2.read().decode()}"
                                )
                        except Exception as save_error:
                            current_app.logger.warning(
                                f"Error saving grace_job_id to file: {str(save_error)}"
                            )

                        # Close SSH connection after saving grace_job_id
                        ssh.close()

                        # Update cache with submitted status
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "grace_job_id": grace_job_id,
                                "status": "submitted",
                            }
                        )
                        current_app.logger.info(
                            f"Job {job_id} submitted successfully with Grace job ID {grace_job_id}"
                        )
                    else:
                        # Close SSH connection before handling error
                        ssh.close()
                        # Update cache with failed status
                        JOB_STATUS_CACHE[job_id].update(
                            {
                                "status": "failed",
                                "error": f"Failed to submit job: {error}",
                            }
                        )
                        current_app.logger.error(
                            f"Job {job_id} submission failed: {error}"
                        )
                        # Check if error is related to disk quota
                        if "quota" in error.lower() or "disk" in error.lower():
                            JOB_STATUS_CACHE[job_id].update(
                                {
                                    "status": "failed",
                                    "error": f"Disk quota issue detected: {error}. Please clean up old job files.",
                                }
                            )
                        # Check if error is related to memory / resources (Grace out of memory)
                        elif any(
                            x in error.lower()
                            for x in (
                                "memory",
                                "insufficient",
                                "resources",
                                "reason=none",
                            )
                        ):
                            JOB_STATUS_CACHE[job_id].update(
                                {
                                    "status": "failed",
                                    "error": f"Grace HPC is out of memory or resources: {error}. Jobs may be pending; try again later or reduce job requirements.",
                                }
                            )
                except Exception as e:
                    import traceback

                    error_traceback = traceback.format_exc()
                    error_msg = str(e)
                    current_app.logger.error(
                        f"Error in background submission for job {job_id}: {error_msg}"
                    )
                    current_app.logger.error(f"Traceback: {error_traceback}")

                    # Close SSH connection if it exists
                    try:
                        if "ssh" in locals() and ssh:
                            ssh.close()
                    except:
                        pass

                    # Check if error is related to disk quota
                    if "quota" in error_msg.lower() or "disk" in error_msg.lower():
                        error_msg = f"Disk quota issue: {error_msg}. Please clean up old job files."

                    # Update cache with error status
                    JOB_STATUS_CACHE[job_id].update(
                        {
                            "status": "failed",
                            "error": f"Backend error: {error_msg}",
                        }
                    )

        # Start background thread
        thread = threading.Thread(target=submit_job_background, daemon=True)
        thread.start()

        # Return immediately with job_id
        return jsonify(
            {
                "job_id": job_id,
                "grace_job_id": None,  # Will be set after submission
                "status": "submitting",  # Initial status
                "message": f"DRN full pipeline job is being submitted. Job ID: {job_id}. Check status endpoint for updates.",
                "parameters": params_data,
            }
        )

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        current_app.logger.error(f"Error in submit_full_drn_pipeline: {str(e)}")
        current_app.logger.error(f"Traceback: {error_traceback}")

        error_response = jsonify(
            {
                "error": f"Backend error: {str(e)}",
                "details": error_traceback if current_app.debug else None,
            }
        )
        origin = request.headers.get("Origin")
        if origin and (
            origin in current_app.config.get("CORS_ORIGINS", [])
            or "localhost:5173" in origin
        ):
            error_response.headers.add("Access-Control-Allow-Origin", origin)
            error_response.headers.add("Access-Control-Allow-Credentials", "true")
        return error_response, 500


@drn_bp.route("/api/drn/full-pipeline/<job_id>/status", methods=["GET", "OPTIONS"])
@cross_origin()
def check_full_pipeline_status(job_id):
    """Check the status of a full DRN pipeline job"""
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        origin = request.headers.get("Origin")
        # Allow the origin if it's in our allowed list or is a Netlify domain
        if origin and (
            origin in current_app.config.get("CORS_ORIGINS", [])
            or "netlify.app" in origin
            or "localhost" in origin
        ):
            response.headers.add("Access-Control-Allow-Origin", origin)
        else:
            response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response
    try:
        # Get cached job info
        job_info = JOB_STATUS_CACHE.get(job_id, {})

        # Log cache state for debugging
        if not job_info:
            current_app.logger.warning(
                f"[Status Check] Job {job_id} - Not found in cache at all! Cache keys: {list(JOB_STATUS_CACHE.keys())[:10]}"
            )
        else:
            current_app.logger.debug(
                f"[Status Check] Job {job_id} - Cache entry found with keys: {list(job_info.keys())}"
            )

        # Check if job already failed (from background thread)
        if job_info.get("status") == "failed":
            return (
                jsonify(
                    {
                        "job_id": job_id,
                        "status": "failed",
                        "error": job_info.get("error", "Job submission failed"),
                        "message": "Job submission failed. Check backend logs for details.",
                    }
                ),
                500,
            )

        grace_job_id = job_info.get("grace_job_id")
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        submitted_at = job_info.get("submitted_at")
        if submitted_at is None:
            # If not in cache, this is likely a very early status check
            # Use current time as fallback, but log a warning
            submitted_at = time.time()
            current_app.logger.warning(
                f"[Status Check] Job {job_id} - submitted_at not found in cache, using current time as fallback"
            )
        time_elapsed = time.time() - submitted_at
        current_app.logger.info(
            f"[Status Check] Job {job_id} - submitted_at: {submitted_at}, current_time: {time.time()}, time_elapsed: {time_elapsed:.2f} seconds ({time_elapsed/60:.2f} minutes)"
        )

        # If background thread is still submitting (no grace_job_id yet) and it's been less than 60 seconds,
        # avoid SSH connection to prevent race condition with background thread that's connecting
        # This handles cases where Duo 2FA takes time to complete
        if not grace_job_id and time_elapsed < 120:
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Background submission still in progress (grace_job_id=None, time_elapsed: {time_elapsed:.2f}s), skipping SSH connection to avoid race condition"
            )
            return jsonify(
                {
                    "job_id": job_id,
                    "grace_job_id": grace_job_id,
                    "status": "submitting",
                    "message": "Job is being submitted to Grace HPC. Please wait...",
                    "submitted_at": submitted_at,
                    "logs": [],
                }
            )

        # Connect to Grace via SSH using pooled connection to avoid repeated DUO authentication
        try:
            ssh = get_ssh_connection_pooled()
        except Exception as ssh_error:
            current_app.logger.error(
                f"SSH connection failed for job {job_id}: {str(ssh_error)} (time_elapsed: {time_elapsed:.2f}s)"
            )
            # If still within submission window, return submitting status
            if time_elapsed < 300:
                return jsonify(
                    {
                        "job_id": job_id,
                        "status": "submitting",
                        "message": "Job is being submitted to Grace HPC. Please wait...",
                    }
                )
            else:
                return (
                    jsonify(
                        {
                            "job_id": job_id,
                            "status": "unknown",
                            "error": f"Failed to connect to Grace HPC: {str(ssh_error)}",
                            "message": "Unable to check job status. Please try again later.",
                        }
                    ),
                    500,
                )

        # If job not in cache or grace_job_id is None, try to recover by checking if job folder exists
        if not grace_job_id:
            current_app.logger.info(
                f"[Status Check] Job {job_id} - grace_job_id is None, attempting recovery..."
            )
            # Check if job folder exists on Grace
            check_folder_cmd = (
                f"test -d {job_folder} && echo 'exists' || echo 'not_found'"
            )
            stdin, stdout, stderr = ssh.exec_command(check_folder_cmd)
            folder_exists = stdout.read().decode().strip()
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Folder check result: '{folder_exists}', job_folder: '{job_folder}'"
            )

            if folder_exists == "not_found":
                # Folder doesn't exist yet - check if still within submission window
                if time_elapsed < 300:  # Less than 5 minutes
                    # Don't close pooled connection
                    return jsonify(
                        {
                            "job_id": job_id,
                            "grace_job_id": None,
                            "status": "submitting",
                            "message": "Job is being submitted to Grace HPC. Please wait...",
                        }
                    )
                else:
                    # Submission timeout
                    # Don't close pooled connection
                    return (
                        jsonify(
                            {
                                "error": "Job not found. The job may have been cleared or never submitted.",
                                "job_id": job_id,
                            }
                        ),
                        404,
                    )

            # Try to recover grace_job_id from saved file first (most reliable)
            read_job_id_cmd = f"cat {job_folder}/.grace_job_id 2>/dev/null || echo ''"
            stdin, stdout, stderr = ssh.exec_command(read_job_id_cmd)
            recovered_job_id = stdout.read().decode().strip()
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Attempted to recover grace_job_id from .grace_job_id file: '{recovered_job_id}'"
            )

            # If not found in file, try to find it from log files (fallback)
            if not recovered_job_id:
                find_job_id_cmd = f"grep -h 'Submitted batch job' {job_folder}/*.out 2>/dev/null | tail -1 | awk '{{print $NF}}'"
                stdin2, stdout2, stderr2 = ssh.exec_command(find_job_id_cmd)
                recovered_job_id = stdout2.read().decode().strip()
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Attempted to recover grace_job_id from logs: '{recovered_job_id}'"
                )

            if recovered_job_id:
                grace_job_id = recovered_job_id

                # If we recovered from logs (not from file), save it to file for future use
                # Check if file exists on Grace
                check_file_cmd = f"test -f {job_folder}/.grace_job_id && echo 'exists' || echo 'not_exists'"
                stdin3, stdout3, stderr3 = ssh.exec_command(check_file_cmd)
                file_exists = stdout3.read().decode().strip()

                if file_exists == "not_exists":
                    # File doesn't exist, save the recovered grace_job_id
                    try:
                        save_job_id_cmd = (
                            f"echo '{grace_job_id}' > {job_folder}/.grace_job_id"
                        )
                        stdin4, stdout4, stderr4 = ssh.exec_command(save_job_id_cmd)
                        exit_status4 = stdout4.channel.recv_exit_status()
                        if exit_status4 == 0:
                            current_app.logger.info(
                                f"Saved recovered grace_job_id {grace_job_id} to {job_folder}/.grace_job_id"
                            )
                    except Exception as save_error:
                        current_app.logger.warning(
                            f"Error saving recovered grace_job_id to file: {str(save_error)}"
                        )

                # Reconstruct job_info and cache it, preserving existing cache data
                existing_job_info = JOB_STATUS_CACHE.get(job_id, {})
                job_info = {
                    "job_id": job_id,
                    "grace_job_id": grace_job_id,
                    "job_folder": job_folder,
                    "submitted_at": existing_job_info.get(
                        "submitted_at", time.time()
                    ),  # Preserve submitted_at
                    "status": existing_job_info.get(
                        "status", "submitted"
                    ),  # Preserve status
                    "parameters": existing_job_info.get(
                        "parameters"
                    ),  # Preserve parameters
                    "output_dir": existing_job_info.get(
                        "output_dir"
                    ),  # Preserve output_dir
                }
                JOB_STATUS_CACHE[job_id] = job_info
                current_app.logger.info(
                    f"Recovered job {job_id} with grace_job_id {grace_job_id} (preserved submitted_at: {job_info.get('submitted_at')})"
                )
            else:
                # Job folder exists but no job_id found - check completion status first
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - No grace_job_id found in logs, checking completion status..."
                )

                # Check multiple indicators of completion
                # 1. Check for .completed marker file
                try:
                    check_completion_cmd = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
                    stdin, stdout, stderr = ssh.exec_command(check_completion_cmd)
                    exit_status = stdout.channel.recv_exit_status()
                    completion_status = stdout.read().decode().strip()
                    stderr_content = stderr.read().decode().strip()
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Completion marker check: '{completion_status}', exit_status: {exit_status}, stderr: '{stderr_content}'"
                    )
                except Exception as e:
                    current_app.logger.warning(
                        f"[Status Check] Job {job_id} - Error checking completion marker: {str(e)}"
                    )
                    completion_status = "not_completed"

                # 2. Also check if output files exist (alternative completion indicator)
                try:
                    check_output_cmd = f"ls {job_folder}/output 2>/dev/null | head -1 && echo 'has_output' || echo 'no_output'"
                    stdin2, stdout2, stderr2 = ssh.exec_command(check_output_cmd)
                    exit_status2 = stdout2.channel.recv_exit_status()
                    output_check = stdout2.read().decode().strip()
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Output check: '{output_check}', exit_status: {exit_status2}"
                    )
                except Exception as e:
                    current_app.logger.warning(
                        f"[Status Check] Job {job_id} - Error checking output: {str(e)}"
                    )
                    output_check = "no_output"

                # 3. Check log files for completion messages
                try:
                    check_log_cmd = f"grep -l 'All steps completed\\|Job completed\\|Pipeline completed' {job_folder}/*.out 2>/dev/null | head -1 && echo 'has_completion_log' || echo 'no_completion_log'"
                    stdin3, stdout3, stderr3 = ssh.exec_command(check_log_cmd)
                    exit_status3 = stdout3.channel.recv_exit_status()
                    log_check = stdout3.read().decode().strip()
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Log completion check: '{log_check}', exit_status: {exit_status3}"
                    )
                except Exception as e:
                    current_app.logger.warning(
                        f"[Status Check] Job {job_id} - Error checking logs: {str(e)}"
                    )
                    log_check = "no_completion_log"

                # If any completion indicator is found, mark as completed
                is_completed = (
                    completion_status == "completed"
                    or "has_output" in output_check
                    or "has_completion_log" in log_check
                )

                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Overall completion check result: is_completed={is_completed}"
                )

                if is_completed:
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Job is COMPLETED (found completion indicator)"
                    )
                    # Don't close pooled connection
                    return jsonify(
                        {
                            "job_id": job_id,
                            "status": "completed",
                            "message": "Job completed (recovered from Grace filesystem)",
                        }
                    )
                else:
                    # Job folder exists but no job_id and not completed
                    # Check if still within submission window
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Not completed, time_elapsed: {time_elapsed}s"
                    )
                    if time_elapsed < 300:  # Less than 5 minutes
                        # Still submitting - SLURM job hasn't been submitted yet
                        current_app.logger.info(
                            f"[Status Check] Job {job_id} - Returning 'submitting' (time_elapsed < 300s)"
                        )
                        # Don't close pooled connection
                        return jsonify(
                            {
                                "job_id": job_id,
                                "grace_job_id": None,
                                "status": "submitting",
                                "message": "Job is being submitted to Grace HPC. Please wait...",
                            }
                        )
                    else:
                        # Submission timeout - something went wrong
                        current_app.logger.warning(
                            f"[Status Check] Job {job_id} - Time elapsed > 300s but no completion marker and no grace_job_id"
                        )
                        # Don't close pooled connection
                        return (
                            jsonify(
                                {
                                    "error": "Job found on Grace but status cannot be determined. The job may be in an unknown state.",
                                    "job_id": job_id,
                                }
                            ),
                            500,
                        )

        # Ensure grace_job_id is not None before checking SLURM
        # But first, check if job is completed even without grace_job_id
        if not grace_job_id:
            # Check if job is completed before assuming it's still submitting
            check_completion_cmd = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
            stdin, stdout, stderr = ssh.exec_command(check_completion_cmd)
            completion_check = stdout.read().decode().strip()
            current_app.logger.info(
                f"[Status Check] Job {job_id} - grace_job_id is None, checking completion: '{completion_check}'"
            )

            if completion_check == "completed":
                # Job is completed even though we don't have grace_job_id
                # Don't close pooled connection
                return jsonify(
                    {
                        "job_id": job_id,
                        "grace_job_id": None,
                        "status": "completed",
                        "message": "Job completed (recovered from Grace filesystem)",
                    }
                )

            # Job not completed, check time elapsed
            if time_elapsed < 300:  # Less than 5 minutes
                # Don't close pooled connection
                return jsonify(
                    {
                        "job_id": job_id,
                        "grace_job_id": None,
                        "status": "submitting",
                        "message": "Job is being submitted to Grace HPC. Please wait...",
                    }
                )
            else:
                # Time elapsed > 5 minutes but no grace_job_id and not completed
                # Don't close pooled connection
                return jsonify(
                    {
                        "job_id": job_id,
                        "grace_job_id": None,
                        "status": "unknown",
                        "message": "Job status cannot be determined. The job may be in an unknown state.",
                    }
                )

        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        cached_status = job_info.get("status")

        # Initialize status with a default value to prevent undefined
        status = "unknown"

        # If job was just submitted successfully (has grace_job_id and cached status is "submitted"),
        # check SLURM first to see if it's already progressed, then return appropriate status
        if grace_job_id and cached_status == "submitted":
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Job successfully submitted with grace_job_id {grace_job_id}, checking SLURM status"
            )
            # Check SLURM to see if it's moved to pending/running/completed
            squeue_cmd = f"squeue -j {grace_job_id} --format='%T' --noheader"
            stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
            slurm_status_raw = stdout.read().decode().strip()
            slurm_status = slurm_status_raw.split("\n")[0] if slurm_status_raw else ""

            if slurm_status:
                # Job is already in queue, use SLURM status (don't go back to "submitted")
                status_map = {
                    "PENDING": "pending",
                    "RUNNING": "running",
                    "COMPLETED": "completed",
                    "FAILED": "failed",
                    "CANCELLED": "failed",
                    "TIMEOUT": "failed",
                    "OUT_OF_MEMORY": "failed",
                    "OUT_OF_MEMMORY": "failed",
                    "OUT_OF_ME+": "failed",  # OUT_OF_MEMORY truncated by sacct
                }
                status = status_map.get(slurm_status, "submitted")
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Found in SLURM queue: '{slurm_status}' -> '{status}' (not changing back to submitted)"
                )
                # Update cache with the actual status
                job_info["status"] = status
                JOB_STATUS_CACHE[job_id] = job_info
            else:
                # Not in queue yet, but we know it was submitted - return "submitted"
                status = "submitted"
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Not in SLURM queue yet, returning 'submitted' status"
                )

            # Return immediately with current status
            return jsonify(
                {
                    "job_id": job_id,
                    "grace_job_id": grace_job_id,
                    "status": status,
                    "submitted_at": submitted_at,
                    "logs": [],
                    "message": (
                        f"Job submitted successfully to Grace HPC with job ID {grace_job_id}"
                        if status == "submitted"
                        else None
                    ),
                }
            )

        # If cached status is already "running", "pending", or "completed", don't override it
        # Continue with normal SLURM checking logic below
        if grace_job_id and cached_status in ["running", "pending", "completed"]:
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Cached status is already '{cached_status}', proceeding with normal status check"
            )

        # Ensure grace_job_id is valid before using it
        if not grace_job_id or not isinstance(grace_job_id, str):
            current_app.logger.warning(
                f"Invalid grace_job_id for job {job_id}: {grace_job_id}"
            )
            # Check if job is completed before assuming it's still submitting
            check_completion_cmd = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
            stdin, stdout, stderr = ssh.exec_command(check_completion_cmd)
            completion_check = stdout.read().decode().strip()
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Invalid grace_job_id, checking completion: '{completion_check}'"
            )

            if completion_check == "completed":
                # Don't close pooled connection
                return jsonify(
                    {
                        "job_id": job_id,
                        "grace_job_id": None,
                        "status": "completed",
                        "message": "Job completed (recovered from Grace filesystem)",
                    }
                )

            # Don't close pooled connection
            return jsonify(
                {
                    "job_id": job_id,
                    "grace_job_id": None,
                    "status": "submitting",
                    "message": "Job is being submitted to Grace HPC. Please wait...",
                }
            )

        # Check SLURM job status
        squeue_cmd = f"squeue -j {grace_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status_raw = stdout.read().decode().strip()
        slurm_status = slurm_status_raw.split("\n")[0] if slurm_status_raw else ""

        current_app.logger.info(
            f"[Status Check] Job {job_id} - squeue result: '{slurm_status_raw}' -> parsed: '{slurm_status}'"
        )

        # Map SLURM status (OUT_OF_MEMORY so we report failed, not completed)
        status_map = {
            "PENDING": "pending",
            "RUNNING": "running",
            "COMPLETED": "completed",
            "FAILED": "failed",
            "CANCELLED": "failed",
            "TIMEOUT": "failed",
            "OUT_OF_MEMORY": "failed",
            "OUT_OF_MEMMORY": "failed",
            "OUT_OF_ME+": "failed",  # OUT_OF_MEMORY truncated by sacct
        }

        if slurm_status:
            # Job found in queue
            status = status_map.get(slurm_status, "unknown")
            # OUT_OF_ME+ is sacct's truncated display of OUT_OF_MEMORY
            if status == "unknown" and slurm_status.startswith("OUT_OF_ME"):
                status = "failed"
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Found in SLURM queue with status: '{slurm_status}' -> mapped to: '{status}'"
            )
            # If status is unknown from SLURM, check if job actually completed
            if status == "unknown":
                check_cmd = f"test -f {job_folder}/.completed && echo 'completed' || echo 'unknown'"
                stdin, stdout, stderr = ssh.exec_command(check_cmd)
                output_check = stdout.read().decode().strip()
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Unknown SLURM status, checking completion marker: '{output_check}'"
                )
                if output_check == "completed":
                    status = "completed"
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Completion marker found, setting status to 'completed'"
                    )
        else:
            # Job not in queue - first check if it's completed (most important check)
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Not in SLURM queue, checking completion marker..."
            )
            check_completion_cmd = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
            stdin, stdout, stderr = ssh.exec_command(check_completion_cmd)
            completion_check = stdout.read().decode().strip()
            current_app.logger.info(
                f"[Status Check] Job {job_id} - Completion marker check result: '{completion_check}'"
            )

            if completion_check == "completed":
                # Job is completed - verify with sacct if we have grace_job_id
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Completion marker found, verifying with sacct..."
                )
                if grace_job_id:
                    sacct_cmd = f"sacct -j {grace_job_id} --format=State --noheader --parsable2 2>/dev/null | head -1 | cut -d'|' -f1"
                    stdin2, stdout2, stderr2 = ssh.exec_command(sacct_cmd)
                    sacct_status = stdout2.read().decode().strip()
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - sacct result: '{sacct_status}'"
                    )
                    if sacct_status:
                        status = status_map.get(sacct_status, "completed")
                        if sacct_status.startswith("OUT_OF_ME"):
                            status = "failed"  # OOM; OUT_OF_ME+ is OUT_OF_MEMORY truncated by sacct
                        current_app.logger.info(
                            f"[Status Check] Job {job_id} - sacct status '{sacct_status}' mapped to: '{status}'"
                        )
                    else:
                        status = "completed"  # Completion marker exists, trust it
                        current_app.logger.info(
                            f"[Status Check] Job {job_id} - sacct returned empty, but completion marker exists. Setting status to 'completed'"
                        )
                else:
                    status = "completed"  # Completion marker exists
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - No grace_job_id, but completion marker exists. Setting status to 'completed'"
                    )
            else:
                # Job not completed - check if it was just submitted or is still running
                current_app.logger.info(
                    f"[Status Check] Job {job_id} - Not completed. Cached status: '{cached_status}', time_elapsed: {time_elapsed}s"
                )
                # If cached status is already "running" or "pending", preserve it (don't go back to "submitted")
                if cached_status in ["running", "pending"]:
                    status = cached_status
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Preserving cached status '{cached_status}' (not changing back to submitted)"
                    )
                elif cached_status == "submitted":
                    # Job was just submitted but not in queue yet - check sacct
                    sacct_cmd = f"sacct -j {grace_job_id} --format=State --noheader --parsable2 2>/dev/null | head -1 | cut -d'|' -f1"
                    stdin2, stdout2, stderr2 = ssh.exec_command(sacct_cmd)
                    sacct_status = stdout2.read().decode().strip()
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Checking sacct (cached_status='submitted'): '{sacct_status}'"
                    )

                    if sacct_status:
                        # Job found in sacct - use that status
                        status = status_map.get(sacct_status, "submitted")
                        current_app.logger.info(
                            f"[Status Check] Job {job_id} - sacct status '{sacct_status}' mapped to: '{status}'"
                        )
                    else:
                        # Job not found in either - still transitioning, return "submitted"
                        status = "submitted"
                        current_app.logger.info(
                            f"[Status Check] Job {job_id} - sacct returned empty, setting status to 'submitted'"
                        )
                else:
                    # Unknown cached status - check logs to determine status
                    check_cmd = f"test -f {job_folder}/*.out && echo 'has_logs' || echo 'no_logs'"
                    stdin, stdout, stderr = ssh.exec_command(check_cmd)
                    output_check = stdout.read().decode().strip()
                    current_app.logger.info(
                        f"[Status Check] Job {job_id} - Log file check: '{output_check}'"
                    )
                    if output_check == "has_logs":
                        # Check log files to see if job is actually running
                        log_check = f"tail -n 5 {job_folder}/*.out 2>/dev/null | grep -q 'Step' && echo 'running' || echo 'unknown'"
                        stdin2, stdout2, stderr2 = ssh.exec_command(log_check)
                        log_output = stdout2.read().decode().strip()
                        status = "running" if log_output == "running" else "unknown"
                        current_app.logger.info(
                            f"[Status Check] Job {job_id} - Log check result: '{log_output}' -> status: '{status}'"
                        )
                    else:
                        # No logs yet - might still be submitting
                        if time_elapsed < 300:  # Less than 5 minutes
                            status = "submitting"
                            current_app.logger.info(
                                f"[Status Check] Job {job_id} - No logs, time_elapsed={time_elapsed}s < 300s, setting status to 'submitting'"
                            )
                        else:
                            status = "unknown"
                            current_app.logger.info(
                                f"[Status Check] Job {job_id} - No logs, time_elapsed={time_elapsed}s >= 300s, setting status to 'unknown'"
                            )

        current_app.logger.info(
            f"[Status Check] Job {job_id} - Final status determined: '{status}'"
        )

        # Get latest log output and detect current step
        logs = []
        current_step = None
        step_progress = None
        error_message = None

        if status in ["running", "completed", "failed"]:
            # Check both .out and .err files for errors
            log_cmd = f"tail -n 100 {job_folder}/*.out 2>/dev/null || echo 'No log file found'"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()

            # Also check .err files for error messages
            err_cmd = f"tail -n 50 {job_folder}/*.err 2>/dev/null || echo ''"
            stdin_err, stdout_err, stderr_err = ssh.exec_command(err_cmd)
            err_content = stdout_err.read().decode()

            # Combine logs and errors
            all_log_content = (
                log_content + "\n" + err_content if err_content else log_content
            )

            # Check for disk quota errors
            if (
                "Disk quota exceeded" in all_log_content
                or "quota exceeded" in all_log_content.lower()
                or "Disk quota" in all_log_content
            ):
                status = "failed"
                error_message = "Disk quota exceeded on Grace HPC. Please clean up old job files or contact the system administrator."
                current_app.logger.error(
                    f"[Status Check] Job {job_id} - Disk quota exceeded detected in logs"
                )

            # Check for out-of-memory (OOM) so we don't report completed when job was killed
            oom_indicators = [
                "out of memory",
                "out-of-memory",
                "outofmemory",
                "memoryerror",
                "killed",
                "cannot allocate memory",
                "memory fault",
                "oom",
                "slurmstepd: error: detected 1 oom-kill",
                "oom-kill",
            ]
            all_log_lower = all_log_content.lower()
            if not error_message and any(
                ind in all_log_lower for ind in oom_indicators
            ):
                oom_found = next(ind for ind in oom_indicators if ind in all_log_lower)
                if status == "completed":
                    status = "failed"
                error_message = (
                    "Job ran out of memory (OOM) on Grace HPC. "
                    "Try reducing the workload or request more memory for the job."
                )
                current_app.logger.error(
                    f"[Status Check] Job {job_id} - OOM detected in logs: '{oom_found}'"
                )

            # Check for other common errors
            if "ERROR:" in all_log_content or "Error:" in all_log_content:
                # Extract error message from logs
                error_lines = [
                    line
                    for line in all_log_content.split("\n")
                    if "ERROR" in line or "Error:" in line
                ]
                if error_lines and not error_message:
                    # Get the most recent error
                    error_message = error_lines[-1].strip()
                    if len(error_message) > 200:
                        error_message = error_message[:200] + "..."

            if log_content:
                logs = log_content.split("\n")[-20:]  # Last 20 lines

                # Parse logs to detect current step
                # Look for step markers: "Step 1: Site Selection", "Step 2: Sample Interpolation", etc.
                step_names = {
                    1: "Site Selection",
                    2: "Sample Interpolation",
                    3: "DRN Preparation",
                    4: "DRN Run",
                    5: "Compile Results",
                }

                # Check logs in reverse order to find the most recent step marker
                all_logs = log_content.split("\n")
                latest_step = 0

                for log_line in reversed(all_logs):
                    # Look for "Step X:" pattern
                    step_match = re.search(r"Step\s+(\d+):\s*(.+)", log_line)
                    if step_match:
                        step_num = int(step_match.group(1))
                        if step_num > latest_step:
                            latest_step = step_num
                            step_name = step_match.group(2).strip()
                            # Check if step completed (look for "All steps completed" or next step)
                            if "All steps completed" in log_content:
                                current_step = (
                                    f"Step {step_num}: {step_name} (Completed)"
                                )
                                step_progress = {
                                    "step": step_num,
                                    "name": step_name,
                                    "status": "completed",
                                }
                            elif any(
                                f"Step {step_num + 1}:" in line
                                for line in all_logs[all_logs.index(log_line) :]
                            ):
                                # Next step has started, so this one is done
                                current_step = (
                                    f"Step {step_num}: {step_name} (Completed)"
                                )
                                step_progress = {
                                    "step": step_num,
                                    "name": step_name,
                                    "status": "completed",
                                }
                            elif "ERROR" in log_line or any(
                                "ERROR" in line
                                for line in all_logs[
                                    max(
                                        0, all_logs.index(log_line) - 5
                                    ) : all_logs.index(log_line)
                                    + 5
                                ]
                            ):
                                # Step failed
                                current_step = f"Step {step_num}: {step_name} (Failed)"
                                step_progress = {
                                    "step": step_num,
                                    "name": step_name,
                                    "status": "failed",
                                }
                            else:
                                # Step is currently running
                                current_step = f"Step {step_num}: {step_name}"
                                step_progress = {
                                    "step": step_num,
                                    "name": step_name,
                                    "status": "running",
                                }
                            break

                # If no step found but job is running, default to Step 1
                if not current_step and status == "running":
                    current_step = "Step 1: Site Selection"
                    step_progress = {
                        "step": 1,
                        "name": "Site Selection",
                        "status": "running",
                    }

        # Ensure status is always a valid string (never None or undefined)
        # This prevents Firebase errors when frontend tries to update Firestore
        if not status or not isinstance(status, str):
            status = "unknown"

        # Update cache
        job_info["status"] = status
        if current_step:
            job_info["current_step"] = current_step
        if step_progress:
            job_info["step_progress"] = step_progress
        JOB_STATUS_CACHE[job_id] = job_info

        # Don't close pooled connection - keep it for reuse
        # ssh.close()

        response_data = {
            "job_id": job_id,
            "grace_job_id": grace_job_id,
            "status": status,  # Guaranteed to be a valid string
            "submitted_at": job_info.get("submitted_at")
            or time.time(),  # Ensure never None
            "logs": logs or [],  # Ensure logs is always a list
            "current_step": current_step,  # Can be None
            "step_progress": step_progress,  # Can be None
        }

        # Add error message if detected
        if error_message:
            response_data["error"] = error_message
            # Update cache with error status
            job_info["status"] = "failed"
            job_info["error"] = error_message
            JOB_STATUS_CACHE[job_id] = job_info

        return jsonify(response_data)

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        current_app.logger.error(
            f"Error in check_full_pipeline_status for {job_id}: {str(e)}"
        )
        current_app.logger.error(f"Traceback: {error_traceback}")
        return (
            jsonify(
                {
                    "job_id": job_id,
                    "status": "unknown",
                    "error": f"Status check failed: {str(e)}",
                    "details": error_traceback if current_app.debug else None,
                }
            ),
            500,
        )


@drn_bp.route("/api/drn/full-pipeline/<job_id>/results", methods=["GET", "OPTIONS"])
@cross_origin()
def get_full_pipeline_results(job_id):
    """Get the results of a completed full DRN pipeline job"""
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        origin = request.headers.get("Origin")
        # Allow the origin if it's in our allowed list or is a Netlify domain
        if origin and (
            origin in current_app.config.get("CORS_ORIGINS", [])
            or "netlify.app" in origin
            or "localhost" in origin
        ):
            response.headers.add("Access-Control-Allow-Origin", origin)
        else:
            response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Max-Age", "3600")
        return response
    """Get the results of a completed full DRN pipeline job"""
    try:
        # Get cached job info
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        output_dir = job_info.get("output_dir", f"{job_folder}/output")

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Allow access to results even if job is still running
        # Read compiled results
        results = {}

        # Try to read the compiled result pickle file
        # Note: year_run is stored in parameters (converted from month_run)
        params = job_info.get("parameters", {})
        year_run = params.get("year_run", params.get("month_run", 12) / 12.0)
        compile_result_path = f"{output_dir}/data/ode_output/{params.get('monte_count', 0)}/rock_seg_*_ode_seg_*_year_{year_run}_step_{params.get('time_step', 1.0)}/DRN_result_compile.pkl"

        # Find the actual file path
        find_cmd = f"find {output_dir}/data/ode_output -name 'DRN_result_compile.pkl' 2>/dev/null | head -1"
        stdin, stdout, stderr = ssh.exec_command(find_cmd)
        result_file = stdout.read().decode().strip()

        # Check job completion status
        check_cmd = (
            f"test -f {job_folder}/.completed && echo 'completed' || echo 'running'"
        )
        stdin, stdout, stderr = ssh.exec_command(check_cmd)
        job_status = stdout.read().decode().strip()

        if result_file:
            # For now, just indicate that results are available
            # In production, you might want to download and parse the pickle file
            results["result_file"] = result_file
            results["status"] = job_status
            results["message"] = (
                "Results are available. Use download endpoint to retrieve."
            )
        else:
            results["status"] = job_status
            if job_status == "completed":
                results["message"] = "Job completed but result file not found yet."
            else:
                results["message"] = (
                    "Job is still running. Partial results may be available."
                )

            ssh.close()

        return jsonify(
            {
                "job_id": job_id,
                "status": job_status,
                "results": results,
            }
        )

    except Exception as e:
        current_app.logger.error(
            f"Error in get_full_pipeline_results for {job_id}: {str(e)}"
        )
        return jsonify({"error": f"Failed to retrieve results: {str(e)}"}), 500


@drn_bp.route("/api/drn/full-pipeline/<job_id>/pdfs", methods=["GET"])
@cross_origin()
def get_full_pipeline_pdfs(job_id):
    """Get list of PDF files from the output/figure/ode_output/ directory"""
    try:
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        output_dir = job_info.get("output_dir", f"{job_folder}/output")
        pdf_dir = f"{output_dir}/figure/ode_output"

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Allow access to PDFs even if job is still running
        # Find all PDF files in the directory
        find_cmd = f"find {pdf_dir} -name '*.pdf' -type f 2>/dev/null | head -20"
        stdin, stdout, stderr = ssh.exec_command(find_cmd)
        pdf_files = stdout.read().decode().strip().split("\n")
        pdf_files = [f for f in pdf_files if f]  # Remove empty strings

        # For each PDF, we'll need to provide a download URL
        # Since we can't serve files directly, we'll return the paths
        # and create a download endpoint for each
        pdf_list = []
        for pdf_path in pdf_files:
            if pdf_path:
                pdf_name = pdf_path.split("/")[-1]
                pdf_list.append(
                    {
                        "name": pdf_name,
                        "path": pdf_path,
                        "url": f"/api/drn/full-pipeline/{job_id}/pdf/{pdf_name}",
                    }
                )

        ssh.close()

        return jsonify({"pdfs": pdf_list}), 200

    except Exception as e:
        current_app.logger.error(
            f"Error in get_full_pipeline_pdfs for {job_id}: {str(e)}"
        )
        return jsonify({"pdfs": [], "error": str(e)}), 500


@drn_bp.route("/api/drn/full-pipeline/<job_id>/pdf/<pdf_name>", methods=["GET"])
@cross_origin()
def download_full_pipeline_pdf(job_id, pdf_name):
    """Download a specific PDF file"""
    try:
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        output_dir = job_info.get("output_dir", f"{job_folder}/output")
        pdf_dir = f"{output_dir}/figure/ode_output"
        pdf_path = f"{pdf_dir}/{pdf_name}"

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Check if PDF exists
        check_cmd = f"test -f {pdf_path} && echo 'exists' || echo 'not_exists'"
        stdin, stdout, stderr = ssh.exec_command(check_cmd)
        exists_check = stdout.read().decode().strip()

        if exists_check != "exists":
            ssh.close()
            return jsonify({"error": "PDF file not found"}), 404

        # Download the PDF file
        sftp = ssh.open_sftp()
        remote_file = sftp.open(pdf_path, "rb")
        pdf_data = remote_file.read()
        remote_file.close()
        sftp.close()
        ssh.close()

        # Create Flask response with PDF file
        response = make_response(pdf_data)
        response.headers["Content-Type"] = "application/pdf"
        response.headers["Content-Disposition"] = f"inline; filename={pdf_name}"

        return response

    except Exception as e:
        current_app.logger.error(
            f"Error in download_full_pipeline_pdf for {job_id}/{pdf_name}: {str(e)}"
        )
        return jsonify({"error": f"Failed to download PDF: {str(e)}"}), 500


@drn_bp.route("/api/drn/generate-watershed", methods=["POST", "OPTIONS"])
@cross_origin()
def generate_watershed():
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response
    """Generate watershed for selected locations (Step 1 only) - runs locally"""
    try:
        payload = request.get_json()
        coordinates = payload.get("coordinates", [])

        # Validate coordinates
        if not coordinates or not isinstance(coordinates, list) or len(coordinates) < 1:
            return jsonify({"error": "At least 1 coordinate pair is required"}), 400

        for coord in coordinates:
            if not isinstance(coord, list) or len(coord) != 2:
                return (
                    jsonify(
                        {"error": "Invalid coordinate format. Expected [lat, lon]"}
                    ),
                    400,
                )
            lat, lon = coord[0], coord[1]
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                return jsonify({"error": "Latitude and longitude must be numbers"}), 400
            if lat < -90 or lat > 90 or lon < -180 or lon > 180:
                return jsonify({"error": "Coordinates out of valid range"}), 400

        # Run watershed generation locally (similar to outlet compatibility check)
        import tempfile
        import shutil

        temp_output_dir = tempfile.mkdtemp(prefix="watershed_")

        # Create temporary coordinates file
        coords_data = [{"lat": c[0], "lon": c[1]} for c in coordinates]
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp_file:
            json.dump(coords_data, tmp_file)
            tmp_coords_file = tmp_file.name

        try:
            # Find 01_site_selection.py script
            site_selection_paths = [
                os.path.join(_DRN_ROOT, "01_site_selection.py"),
                "01_site_selection.py",
            ]

            site_selection_script = None
            for path in site_selection_paths:
                if os.path.exists(path):
                    site_selection_script = path
                    break

            if not site_selection_script:
                return (
                    jsonify(
                        {
                            "error": "01_site_selection.py not found. Please ensure the script is in the backend directory."
                        }
                    ),
                    500,
                )

            current_app.logger.info(
                f"Generating watersheds using {site_selection_script}"
            )

            # Determine the DRN models directory (where input data is located)
            # The script needs to know where to find input/data and input/shp directories
            # Try multiple possible locations
            possible_paths = [
                # Relative to backend directory
                os.path.abspath(os.path.join(_DRN_ROOT, "Models", "DRN", "R_code")),
                # Absolute path from home directory
                os.path.join(
                    os.path.expanduser("~"),
                    "Desktop",
                    "YaleWork",
                    "Models",
                    "DRN",
                    "R_code",
                ),
                # Grace HPC path
                "/home/yhs5/project/DRN/R_code",
                # Current working directory
                os.path.join(os.getcwd(), "Models", "DRN", "R_code"),
            ]

            drn_models_dir = None
            for path in possible_paths:
                # Check if input/data directory exists (indicates correct path)
                input_data_dir = os.path.join(path, "input", "data")
                if os.path.exists(input_data_dir):
                    drn_models_dir = path
                    current_app.logger.info(
                        f"Found DRN models directory at: {drn_models_dir}"
                    )
                    break

            if not drn_models_dir:
                return (
                    jsonify(
                        {
                            "error": "Could not find DRN models directory with input data. Please ensure the DRN models are accessible."
                        }
                    ),
                    500,
                )

            # Run 01_site_selection.py locally
            cmd = [
                "python3",
                site_selection_script,
                "--coords-file",
                tmp_coords_file,
                "--output-dir",
                temp_output_dir,
                "--script-dir",
                drn_models_dir,
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout for watershed generation
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                current_app.logger.error(f"Watershed generation failed: {error_msg}")
                return (
                    jsonify({"error": f"Failed to generate watersheds: {error_msg}"}),
                    500,
                )

            # Read generated shapefiles and convert to GeoJSON
            watershed_results = {}
            shp_dir = os.path.join(temp_output_dir, "shp")

            if os.path.exists(shp_dir):
                shapefiles = {
                    "sf_ws_all": "sf_ws_all.shp",
                    "sf_river_ode": "sf_river_ode.shp",
                    "sf_river_trib": "sf_river_trib.shp",
                    "sf_river_middle": "sf_river_middle.shp",
                }

                # Try to read sf_river_rock if it exists
                rock_shp = os.path.join(shp_dir, "sf_river_rock.shp")
                if os.path.exists(rock_shp):
                    shapefiles["sf_river_rock"] = "sf_river_rock.shp"

                # Convert shapefiles to GeoJSON using geopandas
                try:
                    import geopandas as gpd

                    for key, filename in shapefiles.items():
                        shp_path = os.path.join(shp_dir, filename)
                        if os.path.exists(shp_path):
                            try:
                                gdf = gpd.read_file(shp_path)
                                # Convert to GeoJSON format
                                watershed_results[key] = json.loads(gdf.to_json())
                            except Exception as e:
                                current_app.logger.warning(
                                    f"Failed to convert {filename} to GeoJSON: {str(e)}"
                                )
                except ImportError:
                    return (
                        jsonify(
                            {
                                "error": "geopandas not available, cannot convert shapefiles to GeoJSON"
                            }
                        ),
                        500,
                    )
                except Exception as e:
                    return (
                        jsonify({"error": f"Error converting shapefiles: {str(e)}"}),
                        500,
                    )

            # Clean up temporary files
            try:
                os.unlink(tmp_coords_file)
                shutil.rmtree(temp_output_dir)
            except:
                pass

            if watershed_results:
                current_app.logger.info(
                    f"Successfully generated {len(watershed_results)} watershed layers"
                )
                return jsonify({"watersheds": watershed_results})
            else:
                return (
                    jsonify({"error": "No watershed files found or converted"}),
                    500,
                )

        except subprocess.TimeoutExpired:
            return (
                jsonify({"error": "Watershed generation timed out"}),
                500,
            )
        except Exception as e:
            import traceback

            error_traceback = traceback.format_exc()
            current_app.logger.error(f"Error generating watersheds: {str(e)}")
            current_app.logger.error(f"Traceback: {error_traceback}")
            return jsonify({"error": f"Backend error: {str(e)}"}), 500
        finally:
            # Clean up temporary files
            try:
                if os.path.exists(tmp_coords_file):
                    os.unlink(tmp_coords_file)
                if os.path.exists(temp_output_dir):
                    shutil.rmtree(temp_output_dir)
            except:
                pass

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        current_app.logger.error(f"Error in generate_watershed: {str(e)}")
        current_app.logger.error(f"Traceback: {error_traceback}")
        return jsonify({"error": f"Backend error: {str(e)}"}), 500

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        current_app.logger.error(f"Error in generate_watershed: {str(e)}")
        current_app.logger.error(f"Traceback: {error_traceback}")
        return jsonify({"error": f"Backend error: {str(e)}"}), 500


@drn_bp.route("/api/drn/watershed/<job_id>/status", methods=["GET", "OPTIONS"])
@cross_origin()
def check_watershed_status(job_id):
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response
    """Check the status of a watershed generation job"""
    try:
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        grace_job_id = job_info.get("grace_job_id")
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )

        if not grace_job_id:
            return jsonify({"error": "Job not found"}), 404

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Check SLURM job status
        squeue_cmd = f"squeue -j {grace_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status_raw = stdout.read().decode().strip()
        slurm_status = slurm_status_raw.split("\n")[0] if slurm_status_raw else ""

        # Map SLURM status (OUT_OF_MEMORY -> failed)
        status_map = {
            "PENDING": "pending",
            "RUNNING": "running",
            "COMPLETED": "completed",
            "FAILED": "failed",
            "CANCELLED": "failed",
            "TIMEOUT": "failed",
            "OUT_OF_MEMORY": "failed",
            "OUT_OF_MEMMORY": "failed",
            "OUT_OF_ME+": "failed",  # OUT_OF_MEMORY truncated by sacct
        }

        if slurm_status:
            status = status_map.get(slurm_status, "unknown")
            if status == "unknown" and slurm_status.startswith("OUT_OF_ME"):
                status = "failed"
        else:
            # Job not in queue, check completion marker
            check_cmd = (
                f"test -f {job_folder}/.completed && echo 'completed' || echo 'unknown'"
            )
            stdin, stdout, stderr = ssh.exec_command(check_cmd)
            output_check = stdout.read().decode().strip()
            status = "completed" if output_check == "completed" else "unknown"

        ssh.close()

        return jsonify(
            {
                "job_id": job_id,
                "grace_job_id": grace_job_id,
                "status": status,
            }
        )

    except Exception as e:
        current_app.logger.error(
            f"Error in check_watershed_status for {job_id}: {str(e)}"
        )
        return jsonify({"error": f"Status check failed: {str(e)}"}), 500


@drn_bp.route("/api/drn/watershed/<job_id>/results", methods=["GET", "OPTIONS"])
@cross_origin()
def get_watershed_results(job_id):
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response
    """Get watershed results (Step 1 shapefiles)"""
    try:
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        output_dir = job_info.get(
            "output_dir", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}/output"
        )

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Read GeoJSON files from Step 1 output
        shapefiles = [
            {"name": "sf_ws_all", "path": f"{output_dir}/shp/sf_ws_all.geojson"},
            {
                "name": "sf_ws_selected",
                "path": f"{output_dir}/shp/sf_ws_selected.geojson",
            },  # Watersheds containing selected points
            {"name": "sf_river_ode", "path": f"{output_dir}/shp/sf_river_ode.geojson"},
            {
                "name": "sf_river_trib",
                "path": f"{output_dir}/shp/sf_river_trib.geojson",
            },
            {
                "name": "sf_river_middle",
                "path": f"{output_dir}/shp/sf_river_middle.geojson",
            },
        ]

        results = {}

        for shp in shapefiles:
            try:
                read_cmd = f"cat {shp['path']} 2>/dev/null"
                stdin, stdout, stderr = ssh.exec_command(read_cmd)
                geojson_content = stdout.read().decode()
                if geojson_content:
                    results[shp["name"]] = json.loads(geojson_content)
            except Exception as e:
                current_app.logger.warning(f"Failed to read {shp['name']}: {str(e)}")
                # Continue with other files

        # Try to get sf_river_rock if available
        try:
            rock_path = f"{output_dir}/shp/sf_river_rock.geojson"
            read_cmd = f"cat {rock_path} 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(read_cmd)
            rock_content = stdout.read().decode()
            if rock_content:
                results["sf_river_rock"] = json.loads(rock_content)
        except:
            pass  # Optional file

        # Try to get point-watershed mapping JSON
        point_watershed_map = None
        try:
            map_path = f"{output_dir}/data/point_watershed_map.json"
            read_cmd = f"cat {map_path} 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(read_cmd)
            map_content = stdout.read().decode()
            if map_content:
                point_watershed_map = json.loads(map_content)
        except:
            pass  # Optional file

        ssh.close()

        response_data = {
            "job_id": job_id,
            "shapefiles": results,
        }

        # Add point-watershed mapping if available
        if point_watershed_map:
            response_data["point_watershed_map"] = point_watershed_map

        return jsonify(response_data)

    except Exception as e:
        current_app.logger.error(
            f"Error in get_watershed_results for {job_id}: {str(e)}"
        )
        return (
            jsonify({"error": f"Failed to retrieve watershed results: {str(e)}"}),
            500,
        )


@drn_bp.route("/api/drn/check-outlet-compatibility", methods=["POST", "OPTIONS"])
@cross_origin()
def check_outlet_compatibility():
    """Check if multiple locations share the same outlet"""
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response

    try:
        payload = request.get_json()
        coordinates = payload.get("coordinates", [])

        # Validate coordinates
        if not coordinates or not isinstance(coordinates, list) or len(coordinates) < 2:
            return (
                jsonify({"error": "At least 2 coordinate pairs are required"}),
                400,
            )

        for coord in coordinates:
            if not isinstance(coord, list) or len(coord) != 2:
                return (
                    jsonify(
                        {"error": "Invalid coordinate format. Expected [lat, lon]"}
                    ),
                    400,
                )
            lat, lon = coord[0], coord[1]
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                return (
                    jsonify({"error": "Latitude and longitude must be numbers"}),
                    400,
                )

        # Run the outlet compatibility check locally on the backend server
        # This is faster and avoids SLURM queue overhead

        # Prepare coordinates data
        coords_data = [{"lat": c[0], "lon": c[1]} for c in coordinates]

        # Create a temporary JSON file for coordinates
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp_file:
            json.dump(coords_data, tmp_file)
            tmp_coords_file = tmp_file.name

        try:
            # Find the script - check in scripts directory or current directory
            script_paths = [
                os.path.join(
                    _DRN_ROOT,
                    "scripts",
                    "00_check_outlet_compatibility.py",
                ),
                os.path.join(_DRN_ROOT, "00_check_outlet_compatibility.py"),
                "scripts/00_check_outlet_compatibility.py",
                "00_check_outlet_compatibility.py",
            ]

            script_path = None
            for path in script_paths:
                if os.path.exists(path):
                    script_path = path
                    break

            if not script_path:
                return (
                    jsonify(
                        {
                            "error": "Outlet compatibility check script not found. Please ensure 00_check_outlet_compatibility.py is in the scripts/ directory."
                        }
                    ),
                    500,
                )

            # Run the script locally using subprocess
            import subprocess

            cmd = ["python3", script_path, "--coords-file", tmp_coords_file]

            current_app.logger.debug(
                f"Running outlet compatibility check: {' '.join(cmd)}"
            )

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60  # 60 second timeout
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                current_app.logger.error(
                    f"Outlet compatibility check failed: {error_msg}"
                )
                return (
                    jsonify(
                        {
                            "error": f"Failed to run outlet compatibility check: {error_msg}"
                        }
                    ),
                    500,
                )

            # Parse JSON output from the script
            output = result.stdout.strip()

            # Extract JSON from output (may have other text mixed in)
            import re

            json_match = re.search(r"\{.*\}", output, re.DOTALL)
            if json_match:
                result_json = json_match.group()
            else:
                result_json = output

            try:
                parsed_result = json.loads(result_json)

                # Check for errors in result
                if "error" in parsed_result:
                    return jsonify(parsed_result), 500

                # If outlets are the same, generate watersheds locally
                if parsed_result.get("same_outlet", False):
                    try:
                        # Create temporary output directory
                        import tempfile
                        import shutil

                        temp_output_dir = tempfile.mkdtemp(prefix="watershed_")

                        # Find 01_site_selection.py script
                        site_selection_paths = [
                            os.path.join(_DRN_ROOT, "01_site_selection.py"),
                            "01_site_selection.py",
                        ]

                        site_selection_script = None
                        for path in site_selection_paths:
                            if os.path.exists(path):
                                site_selection_script = path
                                break

                        if site_selection_script:
                            current_app.logger.info(
                                f"Generating watersheds using {site_selection_script}"
                            )

                            # Run 01_site_selection.py locally
                            cmd = [
                                "python3",
                                site_selection_script,
                                "--coords-file",
                                tmp_coords_file,
                                "--output-dir",
                                temp_output_dir,
                            ]

                            result = subprocess.run(
                                cmd,
                                capture_output=True,
                                text=True,
                                timeout=300,  # 5 minute timeout for watershed generation
                            )

                            if result.returncode == 0:
                                # Read generated shapefiles and convert to GeoJSON
                                watershed_results = {}
                                shp_dir = os.path.join(temp_output_dir, "shp")

                                if os.path.exists(shp_dir):
                                    shapefiles = {
                                        "sf_ws_all": "sf_ws_all.shp",
                                        "sf_river_ode": "sf_river_ode.shp",
                                        "sf_river_trib": "sf_river_trib.shp",
                                        "sf_river_middle": "sf_river_middle.shp",
                                    }

                                    # Try to read sf_river_rock if it exists
                                    rock_shp = os.path.join(
                                        shp_dir, "sf_river_rock.shp"
                                    )
                                    if os.path.exists(rock_shp):
                                        shapefiles["sf_river_rock"] = (
                                            "sf_river_rock.shp"
                                        )

                                    # Convert shapefiles to GeoJSON using geopandas
                                    try:
                                        import geopandas as gpd

                                        for key, filename in shapefiles.items():
                                            shp_path = os.path.join(shp_dir, filename)
                                            if os.path.exists(shp_path):
                                                try:
                                                    gdf = gpd.read_file(shp_path)
                                                    # Convert to GeoJSON format
                                                    watershed_results[key] = json.loads(
                                                        gdf.to_json()
                                                    )
                                                except Exception as e:
                                                    current_app.logger.warning(
                                                        f"Failed to convert {filename} to GeoJSON: {str(e)}"
                                                    )
                                    except ImportError:
                                        current_app.logger.warning(
                                            "geopandas not available, cannot convert shapefiles to GeoJSON"
                                        )
                                    except Exception as e:
                                        current_app.logger.warning(
                                            f"Error converting shapefiles: {str(e)}"
                                        )

                                if watershed_results:
                                    parsed_result["watersheds"] = watershed_results
                                    current_app.logger.info(
                                        f"Successfully generated {len(watershed_results)} watershed layers"
                                    )
                                else:
                                    current_app.logger.warning(
                                        "No watershed files found or converted"
                                    )

                                # Clean up temporary directory
                                try:
                                    shutil.rmtree(temp_output_dir)
                                except:
                                    pass
                            else:
                                error_msg = (
                                    result.stderr or result.stdout or "Unknown error"
                                )
                                current_app.logger.warning(
                                    f"Watershed generation failed: {error_msg}"
                                )
                                # Don't fail the outlet check if watershed generation fails
                        else:
                            current_app.logger.warning(
                                "01_site_selection.py not found, skipping watershed generation"
                            )
                    except subprocess.TimeoutExpired:
                        current_app.logger.warning("Watershed generation timed out")
                    except Exception as e:
                        current_app.logger.warning(
                            f"Error generating watersheds: {str(e)}"
                        )
                        # Don't fail the outlet check if watershed generation fails

                # Success - return result immediately
                return jsonify(parsed_result)

            except json.JSONDecodeError as e:
                current_app.logger.error(f"Failed to parse JSON from outlet check: {e}")
                current_app.logger.error(f"Raw output: {output[:500]}")
                return (
                    jsonify(
                        {
                            "error": f"Failed to parse output from outlet check: {str(e)}",
                            "raw_output": output[:500],  # First 500 chars for debugging
                        }
                    ),
                    500,
                )

        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_coords_file)
            except:
                pass

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        current_app.logger.error(f"Error in 00_check_outlet_compatibility: {str(e)}")
        current_app.logger.error(f"Traceback: {error_traceback}")
        return jsonify({"error": f"Backend error: {str(e)}"}), 500


# SSH connection pool to avoid repeated DUO authentication


@drn_bp.route("/api/drn/check-outlet-compatibility/<job_id>/status", methods=["GET"])
def check_outlet_compatibility_status(job_id):
    """Check the status of an outlet compatibility check job"""
    try:
        # Get cached job info
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        grace_job_id = job_info.get("grace_job_id")
        job_folder = job_info.get("job_folder")

        if not grace_job_id or not job_folder:
            return jsonify({"error": "Job not found"}), 404

        # Use pooled SSH connection to avoid repeated DUO authentication
        ssh = get_ssh_connection_pooled()

        # Check SLURM job status
        squeue_cmd = f"squeue -j {grace_job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status_raw = stdout.read().decode().strip()
        slurm_status = slurm_status_raw.split("\n")[0] if slurm_status_raw else ""

        # Map SLURM status (OUT_OF_MEMORY -> failed)
        status_map = {
            "PENDING": "pending",
            "RUNNING": "running",
            "COMPLETED": "completed",
            "FAILED": "failed",
            "CANCELLED": "cancelled",
            "TIMEOUT": "timeout",
            "OUT_OF_MEMORY": "failed",
            "OUT_OF_MEMMORY": "failed",
            "OUT_OF_ME+": "failed",  # OUT_OF_MEMORY truncated by sacct
        }
        status = status_map.get(slurm_status, "unknown")
        if status == "unknown" and slurm_status.startswith("OUT_OF_ME"):
            status = "failed"

        # If not in queue, check for completion marker
        if not slurm_status:
            check_cmd = (
                f"test -f {job_folder}/.completed && echo 'completed' || echo 'failed'"
            )
            stdin, stdout, stderr = ssh.exec_command(check_cmd)
            completion_status = stdout.read().decode().strip()
            if completion_status == "completed":
                status = "completed"
            elif completion_status == "failed":
                status = "failed"

        # Try to read results from .out file when job is completed
        results = None
        if status == "completed":
            try:
                # Optimize: combine file existence and content check into one command
                # This reduces SSH round trips from 3 to 1
                find_and_read_cmd = f"""
                    LOG_FILE=$(find {job_folder} -name 'outlet_check_*.out' -type f | head -1)
                    if [ -n "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
                        tail -50 "$LOG_FILE" 2>&1
                    fi
                """
                stdin, stdout, stderr = ssh.exec_command(find_and_read_cmd)
                full_output = stdout.read().decode().strip()

                if full_output:
                    # Extract JSON output
                    lines = full_output.split("\n")
                    json_output = None

                    # Find the line index where the JSON starts (contains "same_outlet")
                    json_start_idx = None
                    for i, line in enumerate(lines):
                        if '"same_outlet"' in line:
                            # Found the JSON, now find where it starts (opening brace)
                            for j in range(i, -1, -1):
                                if lines[j].strip().startswith("{"):
                                    json_start_idx = j
                                    break
                            break

                    if json_start_idx is not None:
                        json_lines = lines[json_start_idx:]
                        json_candidate = "\n".join(json_lines).strip()

                        brace_count = 0
                        last_brace_idx = -1
                        for i, char in enumerate(json_candidate):
                            if char == "{":
                                brace_count += 1
                            elif char == "}":
                                brace_count -= 1
                                if brace_count == 0:
                                    last_brace_idx = i
                                    break

                        if last_brace_idx >= 0:
                            json_candidate = json_candidate[: last_brace_idx + 1]

                        try:
                            parsed = json.loads(json_candidate)
                            if "same_outlet" in parsed:
                                results = parsed
                        except:
                            pass
            except Exception as e:
                # If we can't read results here, that's okay - frontend will fetch separately
                current_app.logger.debug(
                    f"Could not read results in status check: {str(e)}"
                )

        # Update cache
        job_info["status"] = status
        JOB_STATUS_CACHE[job_id] = job_info

        # Don't close SSH connection - keep it for reuse
        # ssh.close()

        response_data = {
            "job_id": job_id,
            "grace_job_id": grace_job_id,
            "status": status,
        }

        # Include results if we have them
        if results:
            response_data["results"] = results

        return jsonify(response_data)

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        return (
            jsonify({"error": f"Error checking outlet compatibility status: {str(e)}"}),
            500,
        )


@drn_bp.route("/api/drn/check-outlet-compatibility/<job_id>/results", methods=["GET"])
def get_outlet_compatibility_results(job_id):
    """Get the results of a completed outlet compatibility check job"""
    try:
        # Get cached job info
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get("job_folder")

        if not job_folder:
            return jsonify({"error": "Job not found"}), 404

        # Use pooled SSH connection to avoid repeated DUO authentication
        ssh = get_ssh_connection_pooled()

        # Check if job is completed
        check_cmd = f"test -f {job_folder}/.completed && echo 'completed' || echo 'not_completed'"
        stdin, stdout, stderr = ssh.exec_command(check_cmd)
        completion_status = stdout.read().decode().strip()

        if completion_status != "completed":
            # Don't close SSH connection - keep it for reuse
            return jsonify({"error": "Job not completed yet", "status": "running"}), 202

        # First, try to read from result.json file (created by the batch script)
        result_file = f"{job_folder}/result.json"
        read_result_cmd = f"cat {result_file} 2>/dev/null"
        stdin, stdout, stderr = ssh.exec_command(read_result_cmd)
        result_json = stdout.read().decode().strip()

        if result_json:
            try:
                parsed = json.loads(result_json)
                if "same_outlet" in parsed or "error" in parsed:
                    return jsonify(parsed)
            except json.JSONDecodeError:
                pass  # Fall through to log file parsing

        # Fallback: Read output from log file
        find_cmd = f"find {job_folder} -name 'outlet_check_*.out' -type f | head -1"
        stdin, stdout, stderr = ssh.exec_command(find_cmd)
        log_file = stdout.read().decode().strip()

        if not log_file:
            # Don't close SSH connection - keep it for reuse
            return jsonify({"error": "Log file not found"}), 404

        read_cmd = f"cat {log_file} 2>&1"
        stdin, stdout, stderr = ssh.exec_command(read_cmd)
        full_output = stdout.read().decode().strip()

        # Extract JSON output - the JSON is at the end of the log
        lines = full_output.split("\n")
        json_output = None

        # Find the line index where the JSON starts (contains "same_outlet")
        json_start_idx = None
        for i, line in enumerate(lines):
            if '"same_outlet"' in line:
                # Found the JSON, now find where it starts (opening brace)
                for j in range(i, -1, -1):
                    if lines[j].strip().startswith("{"):
                        json_start_idx = j
                        break
                break

        if json_start_idx is not None:
            # Extract JSON from start to end
            json_lines = lines[json_start_idx:]
            json_candidate = "\n".join(json_lines).strip()

            # Remove any trailing text after the closing brace
            brace_count = 0
            last_brace_idx = -1
            for i, char in enumerate(json_candidate):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        last_brace_idx = i
                        break

            if last_brace_idx >= 0:
                json_candidate = json_candidate[: last_brace_idx + 1]

            try:
                parsed = json.loads(json_candidate)
                if "same_outlet" in parsed:
                    json_output = json_candidate
            except json.JSONDecodeError:
                pass

        # Fallback: search for any valid JSON object in reverse
        if not json_output:
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if line.startswith("{") and '"same_outlet"' in line:
                    try:
                        parsed = json.loads(line)
                        if "same_outlet" in parsed:
                            json_output = line
                            break
                    except:
                        pass

        # Don't close SSH connection - keep it for reuse
        # ssh.close()

        if not json_output:
            return jsonify({"error": "Could not extract JSON from log file"}), 500

        # Parse and return the result
        try:
            result = json.loads(json_output)
            if "error" in result:
                return (
                    jsonify(
                        {
                            "error": result["error"],
                            "outlet_comids": result.get("outlet_comids", []),
                        }
                    ),
                    500,
                )
            else:
                return jsonify(result)
        except json.JSONDecodeError:
            return (
                jsonify(
                    {
                        "error": f"Invalid JSON response from outlet check script: {json_output[:200]}"
                    }
                ),
                500,
            )

    except Exception as e:
        import traceback

        error_traceback = traceback.format_exc()
        return (
            jsonify({"error": f"Error getting outlet compatibility results: {str(e)}"}),
            500,
        )


@drn_bp.route("/api/drn/full-pipeline/<job_id>/site-selection-results", methods=["GET"])
@cross_origin()
def get_full_pipeline_site_selection_results(job_id):
    """Get site selection results (Step 1) from a full pipeline job"""
    try:
        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        output_dir = job_info.get("output_dir", f"{job_folder}/output")

        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Read GeoJSON files from Step 1 output (same structure as site-selection endpoint)
        shapefiles = [
            {"name": "sf_ws_all", "path": f"{output_dir}/shp/sf_ws_all.geojson"},
            {"name": "sf_river_ode", "path": f"{output_dir}/shp/sf_river_ode.geojson"},
            {
                "name": "sf_river_trib",
                "path": f"{output_dir}/shp/sf_river_trib.geojson",
            },
            {
                "name": "sf_river_middle",
                "path": f"{output_dir}/shp/sf_river_middle.geojson",
            },
        ]

        results = {}

        for shp in shapefiles:
            try:
                read_cmd = f"cat {shp['path']} 2>/dev/null"
                stdin, stdout, stderr = ssh.exec_command(read_cmd)
                geojson_content = stdout.read().decode()
                if geojson_content:
                    results[shp["name"]] = json.loads(geojson_content)
            except Exception as e:
                current_app.logger.warning(f"Failed to read {shp['name']}: {str(e)}")
                # Continue with other files

        # Try to get sf_river_rock if available
        try:
            rock_path = f"{output_dir}/shp/sf_river_rock.geojson"
            read_cmd = f"cat {rock_path} 2>/dev/null"
            stdin, stdout, stderr = ssh.exec_command(read_cmd)
            rock_content = stdout.read().decode()
            if rock_content:
                results["sf_river_rock"] = json.loads(rock_content)
        except:
            pass  # Optional file

        ssh.close()

        return jsonify(
            {
                "job_id": job_id,
                "shapefiles": results,
            }
        )

    except Exception as e:
        current_app.logger.error(
            f"Error in get_full_pipeline_site_selection_results for {job_id}: {str(e)}"
        )
        return (
            jsonify({"error": f"Failed to retrieve site selection results: {str(e)}"}),
            500,
        )


@drn_bp.route("/api/drn/full-pipeline/<job_id>/download", methods=["GET", "OPTIONS"])
@cross_origin()
def download_full_pipeline_results(job_id):
    # Handle preflight OPTIONS request
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add(
            "Access-Control-Allow-Headers",
            "Content-Type, ngrok-skip-browser-warning, Authorization, X-Requested-With",
        )
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        response.headers.add("Access-Control-Max-Age", "3600")
        return response
    """Download results as a zip file"""
    try:
        import zipfile
        import tempfile
        import io

        job_info = JOB_STATUS_CACHE.get(job_id, {})
        job_folder = job_info.get(
            "job_folder", f"/home/{GRACE_USER}/project/DRN/jobs/{job_id}"
        )
        output_dir = job_info.get("output_dir", f"{job_folder}/output")

        # Connect to Grace via SSH using pooled connection to avoid repeated DUO authentication
        try:
            ssh = get_ssh_connection_pooled()
        except Exception as ssh_error:
            error_msg = str(ssh_error)
            current_app.logger.error(
                f"SSH connection failed for download {job_id}: {error_msg}"
            )
            if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
                return (
                    jsonify(
                        {
                            "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                            "job_id": job_id,
                        }
                    ),
                    429,  # Too Many Requests
                )
            return (
                jsonify(
                    {
                        "error": f"Failed to connect to Grace HPC: {error_msg}",
                        "job_id": job_id,
                    }
                ),
                500,
            )

        # Allow download even if job is still running (downloads whatever is available)
        # Create a zip file on Grace and download it
        # Include both output folder and PDF files
        try:
            pdf_dir = f"{output_dir}/figure/ode_output"

            # Create zip with output folder (which includes PDFs in output/figure/ode_output/)
            # The -r flag recursively includes all subdirectories, so PDFs should be included
            zip_cmd = f"cd {job_folder} && zip -r results.zip output/ 2>&1"
            stdin, stdout, stderr = ssh.exec_command(zip_cmd, timeout=120)
            exit_status = stdout.channel.recv_exit_status()
            zip_output = stdout.read().decode()
            error_msg = stderr.read().decode()

            if exit_status != 0:
                current_app.logger.error(
                    f"Failed to create zip for {job_id}: {error_msg}"
                )
                # Don't close pooled connection
                return jsonify({"error": f"Failed to create zip: {error_msg}"}), 500

            # Verify PDFs are included by checking if PDF directory exists and counting PDFs
            check_pdf_cmd = f"test -d {pdf_dir} && find {pdf_dir} -name '*.pdf' -type f 2>/dev/null | wc -l || echo '0'"
            stdin_pdf, stdout_pdf, stderr_pdf = ssh.exec_command(check_pdf_cmd)
            pdf_count = stdout_pdf.read().decode().strip()

            # Verify PDFs are in the zip file
            verify_zip_cmd = f"cd {job_folder} && unzip -l results.zip 2>/dev/null | grep -c '\.pdf$' || echo '0'"
            stdin_verify, stdout_verify, stderr_verify = ssh.exec_command(
                verify_zip_cmd
            )
            pdfs_in_zip = stdout_verify.read().decode().strip()

            if pdf_count and pdf_count != "0":
                current_app.logger.info(
                    f"Job {job_id}: Found {pdf_count} PDF files in {pdf_dir}, {pdfs_in_zip} PDFs in zip file"
                )
                if pdfs_in_zip == "0" or int(pdfs_in_zip) < int(pdf_count):
                    # PDFs not fully included, add them explicitly
                    # Use relative path from job_folder: output/figure/ode_output/
                    current_app.logger.warning(
                        f"Job {job_id}: Not all PDFs in zip ({pdfs_in_zip}/{pdf_count}), adding them explicitly"
                    )
                    add_pdf_cmd = f"cd {job_folder} && zip -r results.zip output/figure/ode_output/*.pdf 2>&1"
                    stdin_add, stdout_add, stderr_add = ssh.exec_command(
                        add_pdf_cmd, timeout=60
                    )
                    add_exit_status = stdout_add.channel.recv_exit_status()
                    add_error = stderr_add.read().decode()
                    if add_exit_status != 0:
                        current_app.logger.warning(
                            f"Job {job_id}: Failed to explicitly add PDFs: {add_error}, but continuing with existing zip"
                        )
                    else:
                        current_app.logger.info(
                            f"Job {job_id}: Successfully added PDFs to zip file"
                        )
            else:
                current_app.logger.info(
                    f"Job {job_id}: No PDF files found in {pdf_dir}"
                )

            # Download the zip file
            sftp = ssh.open_sftp()
            zip_path = f"{job_folder}/results.zip"

            # Read zip file into memory
            remote_file = sftp.open(zip_path, "rb")
            zip_data = remote_file.read()
            remote_file.close()
            sftp.close()
            # Don't close pooled connection - keep it for reuse
        except paramiko.SSHException as ssh_err:
            error_msg = str(ssh_err)
            current_app.logger.error(
                f"SSH error during download for {job_id}: {error_msg}"
            )
            if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
                return (
                    jsonify(
                        {
                            "error": "Authentication failed. Please wait a minute and try again.",
                            "job_id": job_id,
                        }
                    ),
                    429,
                )
            raise

        # Create Flask response with zip file
        response = make_response(zip_data)
        response.headers["Content-Type"] = "application/zip"
        response.headers["Content-Disposition"] = (
            f"attachment; filename=drn_results_{job_id}.zip"
        )

        return response

    except Exception as e:
        error_msg = str(e)
        current_app.logger.error(
            f"Error in download_full_pipeline_results for {job_id}: {error_msg}"
        )
        import traceback

        current_app.logger.error(f"Traceback: {traceback.format_exc()}")

        # Check for authentication-related errors
        if "Authentication failed" in error_msg or "Anomalous request" in error_msg:
            return (
                jsonify(
                    {
                        "error": "Authentication failed. Please wait a minute and try again. Too many connection attempts may trigger security measures.",
                        "job_id": job_id,
                    }
                ),
                429,  # Too Many Requests
            )

        return jsonify({"error": f"Failed to download results: {error_msg}"}), 500


if __name__ == "__main__":
    current_app.logger.info(f"Starting Flask server on port 8000")
    current_app.logger.info(
        f"CORS origins: {current_app.config.get('CORS_ORIGINS', [])}"
    )
    app.run(host="0.0.0.0", port=8000, debug=True)
