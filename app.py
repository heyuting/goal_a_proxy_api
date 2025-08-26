from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import subprocess
import io
import os
import json
import time
import paramiko
import re
import logging

load_dotenv()  # Load variables from .env file

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)
# Configure CORS origins from environment variable or use defaults
# Read CORS origins from env and strip whitespace/trailing slashes
# Allowed origins from .env
cors_origins = [
    origin.rstrip("/")
    for origin in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,https://venerable-speculoos-b273da.netlify.app",
    ).split(",")
]
app.logger.debug(f"CORS origins configured: {cors_origins}")


# Dynamic origin function for credentials support
def get_cors_origin():
    origin = request.headers.get("Origin")
    if origin in cors_origins:
        return origin
    return None


# Apply CORS globally
CORS(
    app,
    origins=["https://venerable-speculoos-b273da.netlify.app", "http://localhost:5173"],
    allow_headers=["Content-Type", "ngrok-skip-browser-warning"],
    methods=["GET", "POST", "OPTIONS"],
    supports_credentials=True,
)

GRACE_USER = "yhs5"
JOB_STATUS_CACHE = {}  # In production, use Redis or database


@app.before_request
def handle_options():
    if request.method == "OPTIONS":
        return "", 200


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
        # Try Ed25519 first (most common for OpenSSH format)
        private_key = paramiko.Ed25519Key.from_private_key(io.StringIO(private_key_str))
    except:
        try:
            # Try RSA format
            private_key = paramiko.RSAKey.from_private_key(io.StringIO(private_key_str))
        except:
            try:
                # Try ECDSA
                private_key = paramiko.ECDSAKey.from_private_key(
                    io.StringIO(private_key_str)
                )
            except:
                raise Exception("Could not parse SSH private key. Unsupported format.")

    ssh.connect(
        hostname=hostname,
        username=username,
        pkey=private_key,
        timeout=60,
        auth_timeout=60,
    )
    return ssh


@app.route("/api/run-job", methods=["POST"])
def run_job():
    app.logger.debug("Received request to run job")
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
        if model != "drn":
            return jsonify({"error": "Only DRN model supported currently"}), 400
        if not isinstance(parameters, dict):
            return jsonify({"error": "Missing or invalid parameters"}), 400

        # Create unique job folder on Grace server (via SSH)
        timestamp = int(time.time())
        job_folder = f"/home/yhs5/project/DRN/tmp_drn_job_{user_id_clean}_{timestamp}"

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
        sbatch_cmd = (
            f"sbatch /home/{GRACE_USER}/project/DRN/run_drn_job.sh {job_folder}"
        )
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

            return jsonify(
                {
                    "job_id": job_id,
                    "status": "submitted",
                    "message": f"DRN job submitted successfully! Job ID: {job_id}",
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


@app.route("/api/check-job-status/<job_id>", methods=["GET"])
def check_job_status(job_id):
    try:
        # Connect to Grace via SSH
        ssh = get_ssh_connection()

        # Check SLURM job status via SSH
        squeue_cmd = f"squeue -j {job_id} --format='%T' --noheader"
        stdin, stdout, stderr = ssh.exec_command(squeue_cmd)
        slurm_status_raw = stdout.read().decode().strip()
        slurm_status = slurm_status_raw.split("\n")[0]  # Take only first line
        app.logger.debug(f"SLURM status for {job_id}: {slurm_status}")

        if slurm_status:
            # Map SLURM status to our status
            status_map = {
                "PENDING": "pending",
                "RUNNING": "running",
                "COMPLETED": "completed",
                "FAILED": "failed",
                "CANCELLED": "failed",
                "TIMEOUT": "failed",
            }
            status = status_map.get(slurm_status, "unknown")
        else:
            # Job not in queue, check if output file exists to determine completion
            check_cmd = f"test -f /home/{GRACE_USER}/project/DRN/tmp_output/drn_{job_id}_1.out && echo 'completed' || echo 'failed'"
            stdin, stdout, stderr = ssh.exec_command(check_cmd)
            output_check = stdout.read().decode().strip()
            status = output_check  # Will be either 'completed' or 'failed'
            app.logger.debug(f"Status for {job_id}: {status}")
        # Get job logs if available
        logs = []
        if status in ["completed", "failed"]:
            # Try to get job output
            log_cmd = f"cat /home/{GRACE_USER}/project/DRN/tmp_output/drn_{job_id}_1.out 2>/dev/null || echo 'No output file found'"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()
            logs = log_content.split("\n") if log_content else ["No logs available"]
            app.logger.debug(f"Logs for {job_id}: {logs}")
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
        app.logger.error(f"Error in check_job_status for {job_id}: {str(e)}")
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
