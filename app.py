from flask import Flask, request, jsonify
import subprocess
import io
import os
import json
import time
import paramiko

app = Flask(__name__)

GRACE_USER = "yhs5"
JOB_STATUS_CACHE = {}  # In production, use Redis or database


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

    ssh.connect(hostname=hostname, username=username, pkey=private_key)
    return ssh


@app.route("/api/test-ssh", methods=["GET"])
def test_ssh():
    """Test endpoint to verify SSH connection"""
    try:
        # Use the helper function for consistency
        ssh = get_ssh_connection()

        # Run a simple test command
        stdin, stdout, stderr = ssh.exec_command(
            "whoami && hostname && pwd && ls -la /home/yhs5/project/goal_a_scripts/"
        )
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()

        ssh.close()

        return jsonify(
            {
                "success": True,
                "message": "SSH connection successful",
                "test_output": result,
                "error_output": error if error else None,
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/run-job", methods=["POST"])
def run_job():
    try:
        data = request.json
        model = data.get("model")
        parameters = data.get("parameters")
        user_id = data.get("user_id", "anonymous")

        if model != "drn":
            return jsonify({"error": "Only DRN model supported currently"}), 400

        # Create unique job folder on Grace server (via SSH)
        timestamp = int(time.time())
        job_folder = f"/tmp/drn_job_{user_id}_{timestamp}"

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
            f"sbatch /home/{GRACE_USER}/project/goal_a_scripts/run_drn_job.sh {job_folder}"
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

        slurm_status = stdout.read().decode().strip()

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
            # Job not in queue, check if output file exists
            check_cmd = f"ls /home/{GRACE_USER}/drn_{job_id}.out 2>/dev/null && echo 'exists' || echo 'not_found'"
            stdin, stdout, stderr = ssh.exec_command(check_cmd)
            output_check = stdout.read().decode().strip()
            status = "completed" if output_check == "exists" else "failed"

        # Get job logs if available
        logs = []
        if status in ["completed", "failed"]:
            # Try to get job output
            log_cmd = f"tail -20 /home/{GRACE_USER}/drn_{job_id}.out 2>/dev/null || echo 'No output file found'"
            stdin, stdout, stderr = ssh.exec_command(log_cmd)
            log_content = stdout.read().decode()
            logs = log_content.split("\n") if log_content else ["No logs available"]

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
    app.run(host="0.0.0.0", port=5000, debug=True)
