# Goal A Proxy API

A Flask-based proxy API that submits DRN / SCEPTER jobs to Yale Bouchet HPC via SSH (with Duo MFA bridged to the browser) and provides job status monitoring.

## Features

- Submit DRN / SCEPTER jobs to Bouchet via SSH
- Duo MFA prompts surfaced to the web UI (`/api/auth/mfa-*`)
- Real-time job status monitoring using SLURM
- CORS-enabled for Spinup / local frontend integration
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

3. Set up environment variables:

```bash
cp .env.example .env
# edit .env for your Spinup IPs / key path
chmod 600 .env
chmod 600 ~/.ssh/id_ed25519   # or whichever key path you set
```

Auth MFA routes used by the frontend:

- `GET  /api/auth/mfa-status`
- `POST /api/auth/mfa-response`

## Deployment on Yale Spinup (primary)

### Architecture

```
Browser  →  Frontend (Spinup)  →  Proxy API (Spinup)  →  SSH + Duo  →  Bouchet HPC
                              ↘  /api/auth/mfa-*  ↗
```

Spinup VMs are on the Yale network, so the API can SSH to `bouchet.ycrc.yale.edu` without a personal VPN. Duo still applies; the MFA bridge keeps one SSH session in **process memory**.

### 1. Configure `.env` on the API VM

Use `.env.example` as a template. Critical values:

| Variable | Spinup guidance |
| --- | --- |
| `BOUCHET_USER` / `BOUCHET_HOST` | Your HPC username and `bouchet.ycrc.yale.edu` |
| `SSH_PRIVATE_KEY_PATH` | Absolute path to your Yale HPC key **on the Spinup VM** (e.g. `/home/yhs5/.ssh/id_ed25519`) |
| `CORS_ORIGINS` | Exact frontend origin the browser uses (e.g. `http://10.5.203.164` or `http://10.5.203.164:5173`) |
| `MFA_RESPONSE_TIMEOUT_SEC` | How long SSH auth waits for the browser Duo choice (default `150`) |

Legacy `GRACE_USER` / `GRACE_HOST` still work as fallbacks if `BOUCHET_*` is unset.

```bash
chmod 600 .env
chmod 600 /home/yhs5/.ssh/id_ed25519
```

### 2. Point the frontend at the API

Set the frontend API base URL to the Spinup API host, for example:

```text
http://<api-spinup-ip>:8000
```

Include that same frontend origin in `CORS_ORIGINS` on the API.

### 3. Start the API (single worker + threads)

**Required:** one gunicorn worker with multiple threads. MFA status/response must hit the same process that is waiting inside SSH auth.

```bash
./start_api.sh
# equivalent:
# gunicorn --workers 1 --threads 4 --bind 0.0.0.0:8000 --timeout 300 app:app
```

- Bind `0.0.0.0` so other Spinup hosts / browsers can reach the API.
- Do **not** use `--workers 2+`.
- Keep `--timeout` high enough for Duo (e.g. `300`).

### 4. Smoke checks

```bash
curl -s http://127.0.0.1:8000/api/auth/mfa-status
curl -s -X POST http://127.0.0.1:8000/api/test-cors \
  -H "Origin: http://10.5.203.164" -H "Content-Type: application/json"
```

Then trigger a job from the UI and complete Duo in the MFA modal.

## Local development (laptop)

```bash
gunicorn --workers 1 --threads 4 --bind 127.0.0.1:8000 --timeout 300 app:app
# or: python app.py
```

Use `CORS_ORIGINS=http://localhost:5173` and a local `SSH_PRIVATE_KEY_PATH`.

### Optional: ngrok tunnel

Only needed if a remote frontend must reach a laptop-hosted API:

```bash
ngrok http http://localhost:8000
```

Add the ngrok HTTPS origin to `CORS_ORIGINS` and point the frontend at the ngrok URL.