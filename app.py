from flask import Flask, request, jsonify
import subprocess
import os
import json

app = Flask(__name__)

GRACE_USER = "your_grace_username"
JOB_STATUS_CACHE = {}  # In production, use Redis or database

@app.route('/api/run-job', methods=['POST'])
def run_job():
    try:
        data = request.json
        model = data.get('model')
        parameters = data.get('parameters')
        user_id = data.get('user_id', 'anonymous')

        # Create job folder with parameters
        job_folder = f"/tmp/job_{user_id}_{model}_{int(time.time())}"
        os.makedirs(job_folder, exist_ok=True)

        # Write parameters to job folder
        with open(f"{job_folder}/parameters.json", 'w') as f:
            json.dump(parameters, f)

        # Submit to SLURM using sbatch
        cmd = f"sbatch /home/{GRACE_USER}/goal_a_scripts/run_{model}_job.sh {job_folder}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            # Extract job ID from sbatch output
            job_id = result.stdout.strip().split()[-1]

            # Cache job info
            JOB_STATUS_CACHE[job_id] = {
                'model': model,
                'parameters': parameters,
                'user_id': user_id,
                'job_folder': job_folder,
                'status': 'submitted'
            }

            return jsonify({
                'job_id': job_id,
                'status': 'submitted',
                'message': 'Job submitted successfully'
            })
        else:
            return jsonify({
                'error': f'Failed to submit job: {result.stderr}'
            }), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/check-job-status/<job_id>', methods=['GET'])
def check_job_status(job_id):
    try:
        # Check SLURM job status
        cmd = f"squeue -j {job_id} --format='%T' --noheader"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0 and result.stdout.strip():
            slurm_status = result.stdout.strip()

            # Map SLURM status to our status
            status_map = {
                'PENDING': 'pending',
                'RUNNING': 'running',
                'COMPLETED': 'completed',
                'FAILED': 'failed',
                'CANCELLED': 'failed'
            }
            status = status_map.get(slurm_status, 'unknown')
        else:
            # Job not in queue, check if completed
            status = 'completed'  # or 'failed' based on exit code

        # Get job logs if available
        logs = []
        if job_id in JOB_STATUS_CACHE:
            job_folder = JOB_STATUS_CACHE[job_id]['job_folder']
            log_file = f"{job_folder}/slurm-{job_id}.out"
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    logs = f.readlines()[-20:]  # Last 20 lines

        return jsonify({
            'job_id': job_id,
            'status': status,
            'logs': logs,
            'error': None
        })

    except Exception as e:
        return jsonify({
            'job_id': job_id,
            'status': 'unknown',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
