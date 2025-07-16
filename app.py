
from flask import Flask, request, jsonify
import paramiko
import os
import io
import time
import json

app = Flask(__name__)

GRACE_USER = os.getenv("GRACE_USER")
GRACE_HOST = os.getenv("GRACE_HOST")
SSH_KEY = os.getenv("SSH_PRIVATE_KEY").replace("\\n", "\n")


def ssh_connect():
    k = paramiko.Ed25519Key.from_private_key(io.StringIO(SSH_KEY))
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(GRACE_HOST, username=GRACE_USER, pkey=k)
    return client


@app.route('/run-job', methods=['POST'])
def run_job():
    data = request.json
    model = data['model']  # ats, drn, or scepter
    params = data['parameters']

    timestamp = str(int(time.time()))
    job_folder = f"/scratch/{GRACE_USER}/goal_a_jobs/{model}-{timestamp}"
    params_file = f"{job_folder}/params.json"

    client = ssh_connect()
    sftp = client.open_sftp()
    client.exec_command(f"mkdir -p {job_folder}")

    with sftp.file(params_file, 'w') as f:
        f.write(json.dumps(params))
    sftp.close()

    cmd = f"sbatch /home/{GRACE_USER}/goal_a_scripts/run_{model}_job.sh {job_folder}"
    stdin, stdout, stderr = client.exec_command(cmd)
    job_output = stdout.read().decode()
    job_id = job_output.strip().split()[-1]

    client.close()

    return jsonify({"job_id": job_id, "job_folder": job_folder})


@app.route('/check-job-status', methods=['POST'])
def check_job_status():
    job_id = request.json['job_id']
    job_folder = request.json['job_folder']

    client = ssh_connect()
    stdin, stdout, stderr = client.exec_command(f"squeue -j {job_id}")
    output = stdout.read().decode()

    if job_id in output:
        status = "running"
        result = None
    else:
        sftp = client.open_sftp()
        try:
            with sftp.file(f"{job_folder}/output.json", 'r') as f:
                result = json.loads(f.read().decode())
            status = "completed"
        except IOError:
            status = "error"
            result = None
        sftp.close()

    client.close()
    return jsonify({"status": status, "result": result})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
