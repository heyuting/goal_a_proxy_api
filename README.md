# Goal A Proxy API

A Flask-based proxy API that submits DRN jobs to Grace HPC cluster via SSH and provides job status monitoring capabilities.

## Features

- Submit DRN model jobs to Grace HPC cluster via SSH
- Real-time job status monitoring using SLURM queue system
- Support for multiple payload formats (legacy and new data structures)
- CORS-enabled for web frontend integration
- Comprehensive error handling and logging

## API Endpoints

### POST /api/run-job

Submit a DRN model run to Grace HPC.

**Payload Examples:**

```json
{
  "data": {
    "model_type": "drn",
    "locations": [
      {
        "lat": 40.7128,
        "lon": -74.006,
        "ewRiverInputRate": 0.5
      }
    ],
    "numStart": 100,
    "yearRun": 2023,
    "timeStep": 0.1,
    "riverInputRates": [0.5, 0.3],
    "extra": {
      "custom_param": "value"
    }
  },
  "user_id": "user123"
}
```

**Response:**

```json
{
  "job_id": "123456",
  "status": "submitted",
  "message": "DRN job submitted successfully! Job ID: 123456",
  "sbatch_output": "Submitted batch job 123456"
}
```

### GET /api/check-job-status/<job_id>

Check the status of a submitted job.

**Response:**

```json
{
  "job_id": "123456",
  "status": "completed",
  "logs": ["Job completed successfully", "Output saved to..."],
  "slurm_status": "COMPLETED",
  "error": null
}
```

**Status Values:**

- `pending` - Job is queued
- `running` - Job is currently executing
- `completed` - Job finished successfully
- `failed` - Job failed or was cancelled
- `unknown` - Status could not be determined

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd goal_a_proxy_api
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up environment variables (create a `.env` file):

```env
GRACE_USER=your_username
GRACE_HOST=grace.ycrc.yale.edu
SSH_PRIVATE_KEY="-----BEGIN OPENSSH PRIVATE KEY-----\n...\n-----END OPENSSH PRIVATE KEY-----"
CORS_ORIGINS=http://localhost:5173,https://your-frontend-domain.com
```

## Local Development

Run the Flask development server:

```bash
python app.py
```

The API will be available at `http://localhost:8000`

## Deployment

### Architecture Overview

Since cloud services cannot directly access Grace HPC (requires VPN), this setup uses a **local deployment with tunneling**:

1. **Local Machine** - Hosts the Flask API and connects to Grace via VPN
2. **ngrok** - Creates secure tunnel to expose local API to the internet
3. **Grace HPC** - Target cluster for job execution

### Setup Instructions

#### Local Deployment with ngrok

On your local machine that has VPN access to Grace:

1. Install ngrok:

```bash
# Download and install ngrok
# Visit https://ngrok.com/download
```

2. Set up environment variables (create a `.env` file)

3. Run the Flask app locally:

```bash
python app.py
```

4. Create ngrok tunnel:

```bash
ngrok http http://localhost:8000
```

5. Note the ngrok URL (e.g., `https://abc123.ngrok.io`) and use this as your API endpoint

