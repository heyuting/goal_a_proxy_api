# Goal A Proxy API

A Flask-based proxy API that submits DRN / SCEPTER jobs to Yale Bouchet HPC via the system **OpenSSH** client and provides job status monitoring.

Python never loads your private key. OpenSSH reads `~/.ssh/*`, verifies `known_hosts`, runs the protocol, and handles Duo keyboard-interactive auth. The API only runs commands like `ssh bouchet 'sbatch ...'`.

## Features

- Submit DRN / SCEPTER jobs to Bouchet via OpenSSH subprocess
- Duo via OpenSSH ControlMaster (authenticate once on the API host)
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

3. Configure OpenSSH (not Python) for Bouchet:

```bash
cp ssh_config.example ~/.ssh/config
# edit User / IdentityFile
chmod 600 ~/.ssh/config ~/.ssh/id_ed25519
cp .env.example .env
# edit CORS_ORIGINS for your frontend
chmod 600 .env
```

4. Authenticate once (Duo in this terminal):

```bash
./ssh_login_bouchet.sh
```

Status endpoint used by the frontend:

- `GET  /api/auth/mfa-status` — reports whether the OpenSSH ControlMaster is alive
- `POST /api/auth/mfa-response` — not used for Duo with OpenSSH (complete Duo via `ssh_login_bouchet.sh`)

## Deployment on Yale Spinup (primary)

### Architecture

```
Browser → Frontend (Spinup) → Proxy API (Spinup) → OpenSSH (ssh bouchet …) → Bouchet
                                      ↑
                         ControlMaster from ./ssh_login_bouchet.sh
                         (Duo + private key stay in OpenSSH)
```

Spinup VMs are on the Yale network, so the API can reach `bouchet.ycrc.yale.edu`. Duo is completed once in a terminal on the API host; `ControlPersist yes` keeps the multiplexed socket until reboot, `ssh -O exit bouchet`, or Bouchet drops the connection (not a fixed 8-hour timeout).

### 1. OpenSSH on the API VM

```bash
cp ssh_config.example ~/.ssh/config   # Host bouchet …
chmod 600 ~/.ssh/config ~/.ssh/id_ed25519
./ssh_login_bouchet.sh                # complete Duo once
ssh -O check bouchet                  # should succeed while master is up
```

### 2. Configure `.env` on the API VM

| Variable | Spinup guidance |
| --- | --- |
| `BOUCHET_USER` / `BOUCHET_HOST` | HPC username / host (also set in `~/.ssh/config`) |
| `SSH_HOST_ALIAS` | Usually `bouchet` (must match `Host` in ssh config) |
| `CORS_ORIGINS` | Exact frontend origin (e.g. `http://10.5.203.164` or with `:port`) |
| `DRN_MODELS_DIR` | Optional on Spinup now. Absolute path to DRN `R_code` with `input/data` + `input/shp` if you still run COMID/outlet checks locally |

You do **not** need `SSH_PRIVATE_KEY` / `SSH_PRIVATE_KEY_PATH` in `.env` anymore — OpenSSH reads the key from `IdentityFile` in `~/.ssh/config`.

Watershed generation (`POST /api/drn/generate-watershed`) runs on **Bouchet** (16–32G SLURM job). The API returns a `job_id`; the frontend polls `/api/drn/watershed/<job_id>/status` and fetches GeoJSON from `/results`. Keep ControlMaster up (`./ssh_login_bouchet.sh`). National DRN inputs stay on Bouchet under `~/project_pi_par35/yhs5/DRN/R_code` — you do **not** need a 16G Spinup VM for watersheds.

### 3. Point the frontend at the API

```text
http://<api-spinup-ip>:8000
```

Include that frontend origin in `CORS_ORIGINS`.

### 4. Keep the API running with systemd (recommended on Spinup)

Do **not** rely on a laptop terminal with `./start_api.sh` — closing that shell stops gunicorn.

1. Edit `deploy/goal-a-api.service` if your paths differ (`WorkingDirectory`, `User`, venv `ExecStart`).
2. Ensure a venv exists with deps (or point `ExecStart` at system `gunicorn`):

```bash
cd ~/webapp/goal_a_proxy_api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Install and start the service:

```bash
chmod +x deploy/install_systemd_service.sh deploy/check_bouchet_ssh.sh
./deploy/install_systemd_service.sh
```

Useful commands:

```bash
sudo systemctl status goal-a-api
sudo journalctl -u goal-a-api -f
sudo systemctl restart goal-a-api
```

One worker + several threads is intentional. Bouchet auth is **not** managed by systemd — that is OpenSSH ControlMaster (next step).

### 5. Keep Bouchet SSH available

```bash
./ssh_login_bouchet.sh          # once after reboot / when the amber banner appears
ssh -O check bouchet            # Master running (pid=…)
```

Optional health check every 10 minutes (emails only if `NOTIFY_EMAIL` is set and `mail` works):

```bash
crontab -e
# add (env var before the command):
*/10 * * * * NOTIFY_EMAIL=yuting.smeglin@yale.edu /home/yhs5/webapp/goal_a_proxy_api/deploy/check_bouchet_ssh.sh
```

### 6. Smoke checks

```bash
curl -s http://127.0.0.1:8000/api/auth/mfa-status
# expect "available": true / "status": "authenticated" when ControlMaster is up
```

If the frontend amber banner is up, re-run `./ssh_login_bouchet.sh` on the API host.

## Local development (laptop)

```bash
./ssh_login_bouchet.sh
gunicorn --workers 1 --threads 4 --bind 127.0.0.1:8000 --timeout 660 app:app
```

Use `CORS_ORIGINS=http://localhost:5173`.