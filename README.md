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

### POST /api/drn/site-selection

Submit a DRN site selection job (Step 1) to identify watersheds and river networks.

**Request Body:**

```json
{
  "locations": [
    { "lat": 37.0, "lon": -78.0 },
    { "lat": 36.5, "lon": -77.5 }
  ]
}
```

**Response:**

```json
{
  "job_id": "drn_site_1234567890_1234",
  "grace_job_id": "12345",
  "status": "submitted",
  "message": "Site selection job submitted successfully. Job ID: drn_site_1234567890_1234"
}
```

### GET /api/drn/site-selection/<job_id>/status

Check the status of a site selection job.

**Response:**

```json
{
  "job_id": "drn_site_1234567890_1234",
  "grace_job_id": "12345",
  "status": "running",
  "submitted_at": 1234567890.123
}
```

### GET /api/drn/site-selection/<job_id>/results

Get the results of a completed site selection job (returns GeoJSON shapefiles).

**Response:**

```json
{
  "job_id": "drn_site_1234567890_1234",
  "status": "completed",
  "shapefiles": {
    "sf_ws_all": {
      /* GeoJSON watershed polygons */
    },
    "sf_river_ode": {
      /* GeoJSON downstream rivers */
    },
    "sf_river_trib": {
      /* GeoJSON tributaries */
    },
    "sf_river_middle": {
      /* GeoJSON centroids */
    },
    "sf_river_rock": {
      /* GeoJSON selected segments (optional) */
    }
  },
  "summary_csv": "COMID,outlet,Length,ws_area,..."
}
```

If job is still processing, returns `202 Accepted` with:

```json
{
  "status": "processing",
  "message": "Job is still running. Please check back later."
}
```

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
