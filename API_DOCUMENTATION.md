# API Documentation

## DRN Model Endpoints

### 1. Full Pipeline Operations

#### POST `/api/drn/full-pipeline`

Submits a complete DRN pipeline job (Steps 1-5).

**Request Body:**

```json
{
  "coordinates": [[lat1, lon1], [lat2, lon2], ...],
  "rate_rock": 1.0,
  "month_run": 12,
  "time_step": 1.0,
  "feedstock": "carbonate" | "basalt",
  "monte_count": 0
}
```

**Response:**

```json
{
  "job_id": "drn_full_1234567890_1234",
  "grace_job_id": null,
  "status": "submitting",
  "message": "DRN full pipeline job is being submitted...",
  "parameters": { ... }
}
```

**Location:** `blueprints/drn.py:23`

---

#### GET `/api/drn/full-pipeline/{jobId}/status`

Checks the status of a full DRN pipeline job.

**Response:**

```json
{
  "job_id": "...",
  "grace_job_id": "12345",
  "status": "pending" | "running" | "completed" | "failed",
  "submitted_at": 1234567890.123,
  "logs": ["..."],
  "current_step": "Step 1: Site Selection",
  "step_progress": {
    "step": 1,
    "name": "Site Selection",
    "status": "running"
  }
}
```

**Location:** `blueprints/drn.py:423`

---

#### GET `/api/drn/full-pipeline/{jobId}/results`

Fetches job results.

**Response:**

```json
{
  "job_id": "...",
  "status": "completed",
  "results": {
    "result_file": "/path/to/result.pkl",
    "status": "completed",
    "message": "Results are available..."
  }
}
```

**Location:** `blueprints/drn.py:795`

---

#### GET `/api/drn/full-pipeline/{jobId}/download`

Downloads all results as a ZIP file.

**Response:** ZIP file blob (binary)

**Location:** `blueprints/drn.py:2124`

---

#### GET `/api/drn/full-pipeline/{jobId}/site-selection-results`

Gets site selection results (Step 1) from a full pipeline job.

**Response:**

```json
{
  "job_id": "...",
  "shapefiles": {
    "sf_ws_all": { /* GeoJSON */ },
    "sf_river_ode": { /* GeoJSON */ },
    ...
  }
}
```

**Location:** `blueprints/drn.py:2053`

---

#### GET `/api/drn/full-pipeline/{jobId}/pdfs`

Lists available PDF files.

**Response:**

```json
{
  "pdfs": [
    {
      "name": "plot1.pdf",
      "path": "/path/to/plot1.pdf",
      "url": "/api/drn/full-pipeline/{jobId}/pdf/plot1.pdf"
    }
  ]
}
```

**Location:** `blueprints/drn.py:885`

---

#### GET `/api/drn/full-pipeline/{jobId}/pdf/{pdfName}`

Downloads a specific PDF file.

**Response:** PDF file blob (binary)

**Location:** `blueprints/drn.py:933`

---

### 2. Outlet Compatibility Check

#### POST `/api/drn/check-outlet-compatibility`

Checks if multiple locations share the same outlet. Runs synchronously for quick checks.

**Request Body:**

```json
{
  "coordinates": [[lat1, lon1], [lat2, lon2], ...]
}
```

**Response (if same outlet):**

```json
{
  "same_outlet": true,
  "outlet_comids": [12345],
  "unique_outlets": 1,
  "watersheds": {
    /* GeoJSON watersheds */
  }
}
```

**Response (if different outlets):**

```json
{
  "same_outlet": false,
  "outlet_comids": [12345, 67890],
  "unique_outlets": 2
}
```

**Location:** `blueprints/drn.py:1408`

---

#### GET `/api/drn/check-outlet-compatibility/{jobId}/status`

Checks outlet compatibility job status (if submitted as async job).

**Response:**

```json
{
  "job_id": "...",
  "grace_job_id": "12345",
  "status": "completed",
  "results": {
    /* if available */
  }
}
```

**Location:** `blueprints/drn.py:1773`

---

#### GET `/api/drn/check-outlet-compatibility/{jobId}/results`

Fetches outlet compatibility check results.

**Response:**

```json
{
  "same_outlet": true,
  "outlet_comids": [12345],
  "unique_outlets": 1,
  "watersheds": {
    /* if same outlet */
  }
}
```

**Location:** `blueprints/drn.py:1907`

---

### 3. Watershed Generation

#### POST `/api/drn/generate-watershed`

Generates watersheds for selected locations (Step 1 only). Runs locally.

**Request Body:**

```json
{
  "coordinates": [[lat1, lon1], [lat2, lon2], ...]
}
```

**Response:**

```json
{
  "watersheds": {
    "sf_ws_all": {
      /* GeoJSON */
    },
    "sf_river_ode": {
      /* GeoJSON */
    },
    "sf_river_trib": {
      /* GeoJSON */
    },
    "sf_river_middle": {
      /* GeoJSON */
    },
    "sf_river_rock": {
      /* GeoJSON, optional */
    }
  }
}
```

**Location:** `blueprints/drn.py:980`

---

#### GET `/api/drn/watershed/{jobId}/status`

Checks watershed generation job status (if submitted as async job).

**Response:**

```json
{
  "job_id": "...",
  "grace_job_id": "12345",
  "status": "completed"
}
```

**Location:** `blueprints/drn.py:1235`

---

#### GET `/api/drn/watershed/{jobId}/results`

Fetches watershed results.

**Response:**

```json
{
  "job_id": "...",
  "shapefiles": {
    "sf_ws_all": { /* GeoJSON */ },
    "sf_ws_selected": { /* GeoJSON */ },
    "sf_river_ode": { /* GeoJSON */ },
    ...
  },
  "point_watershed_map": { /* optional */ }
}
```

**Location:** `blueprints/drn.py:1307`

---

## SCEPTER Model Endpoints

### Baseline Simulation (Spinup)

#### POST `/api/baseline-simulation`

Submits a baseline SCEPTER spinup job on Bouchet.

**Request Body:**

```json
{
  "coordinate": [lat, lon],
  "location_name": "optional descriptive name"
}
```

**Response:**

```json
{
  "job_id": "baseline_12345",
  "bouchet_job_id": null,
  "status": "submitting",
  "message": "Baseline simulation job is being submitted. Job ID: baseline_12345.",
  "parameters": {
    "coordinate": [lat, lon]
  }
}
```

**Location:** `blueprints/scepter.py:24`

---

#### GET `/api/baseline-simulation/{jobId}/status`

Checks the status of a baseline spinup job.

**Response:**

```json
{
  "job_id": "baseline_12345",
  "bouchet_job_id": "67890",
  "status": "pending" | "running" | "completed" | "failed" | "unknown",
  "submitted_at": 1234567890.123,
  "logs": ["..."]
}
```

**Location:** `blueprints/scepter.py:256`

---

#### GET `/api/baseline-simulation/{jobId}/download`

Downloads all available baseline spinup outputs (the `output/` folder under the job directory) as a ZIP file.

**Response:** ZIP file blob (binary)

**Location:** `blueprints/scepter.py:259`

---

### Run-Model Operations (restart_add_gbas)

#### POST `/api/run-scepter-model`

Submits a SCEPTER run-model job using an existing spinup.

**Request Body:**

```json
{
  "spinup_name": "baseline_12345",
  "restart_name": "my_restart",
  "particle_size": 320,
  "application_rate": 1.0,   // t/ha/yr
  "target_pH": 7.0           // optional
}
```

**Response:**

```json
{
  "job_id": "scepter_run_12345",
  "bouchet_job_id": null,
  "status": "submitting",
  "message": "SCEPTER model run is being submitted. Job ID: scepter_run_12345.",
  "parameters": {
    "spinup_name": "baseline_12345",
    "restart_name": "my_restart",
    "particle_size": 320,
    "application_rate": 100.0,
    "target_pH": "(not set)"
  }
}
```

**Location:** `blueprints/scepter.py:425`

---

#### GET `/api/run-scepter-model/{jobId}/status`

Checks the status of a SCEPTER run-model job.

**Response:**

```json
{
  "job_id": "scepter_run_12345",
  "bouchet_job_id": "67890",
  "status": "pending" | "running" | "completed" | "failed" | "unknown",
  "submitted_at": 1234567890.123,
  "logs": ["..."]
}
```

**Location:** `blueprints/scepter.py:693`

---

#### GET `/api/run-scepter-model/{jobId}/download`

Downloads a ZIP of the job folder for a run-model job (logs, parameters, and any helper files produced under the job directory).

**Response:** ZIP file blob (binary)

**Location:** `blueprints/scepter.py:699`

---

### (Legacy) Full Pipeline Operations

#### POST `/api/scepter/full-pipeline`

Submits a complete SCEPTER pipeline job.

**Request Body:**

```json
{
  "coordinate": [lat, lon],  // Single coordinate point
  "feedstock": "basalt" | "olivine",
  "particle_size": 100 | 320 | 1220 | 3000,  // in µm
  "application_rate": 1.0,  // t/ha/yr
  "target_soil_ph": 7.0  // optional
}
```

**Response:**

```json
{
  "job_id": "scepter_full_1234567890_1234",
  "grace_job_id": null,
  "status": "submitting",
  "message": "SCEPTER full pipeline job is being submitted...",
  "parameters": { ... }
}
```

**Location:** `blueprints/scepter.py:26`

---

#### GET `/api/scepter/full-pipeline/{jobId}/status`

Checks the status of a SCEPTER pipeline job.

**Response:**

```json
{
  "job_id": "...",
  "grace_job_id": "12345",
  "status": "pending" | "running" | "completed" | "failed",
  "submitted_at": 1234567890.123,
  "logs": ["..."],
  "current_step": null,
  "step_progress": null
}
```

**Location:** `blueprints/scepter.py:391`

---

#### GET `/api/scepter/full-pipeline/{jobId}/results`

Fetches job results (lists files in output folder).

**Response:**

```json
{
  "job_id": "...",
  "status": "completed",
  "results": {
    "status": "completed",
    "output_directory": "/path/to/output",
    "files": ["file1.txt", "file2.dat", ...],
    "file_count": 5,
    "message": "Job completed. Results are available..."
  }
}
```

**Location:** `blueprints/scepter.py:731`

---

#### GET `/api/scepter/full-pipeline/{jobId}/download`

Downloads all results as a ZIP file.

**Response:** ZIP file blob (binary)

**Location:** `blueprints/scepter.py:816`

---

## Common Endpoints

### POST `/api/run-job`

Legacy endpoint for running jobs (supports both DRN and SCEPTER).

**Request Body:**

```json
{
  "model": "drn" | "scepter",
  "parameters": { ... },
  "user_id": "user123"
}
```

**Response:**

```json
{
  "job_id": "123456",
  "status": "submitted",
  "message": "DRN job submitted successfully!",
  "sbatch_output": "Submitted batch job 123456"
}
```

**Location:** `blueprints/common.py:15`

---

### GET `/api/check-job-status/{jobId}`

Checks the status of a legacy job.

**Response:**

```json
{
  "job_id": "123456",
  "status": "completed",
  "logs": ["..."],
  "slurm_status": "COMPLETED",
  "error": null
}
```

**Location:** `blueprints/common.py:179`

---

### GET `/api/test-cors`

Test endpoint to verify CORS is working.

**Response:**

```json
{
  "status": "ok",
  "message": "CORS is working!",
  "origin": "http://localhost:5173",
  "cors_origins": [...]
}
```

**Location:** `blueprints/common.py:256`

---

## Status Values

- `submitting` - Job is being submitted to Grace HPC
- `pending` - Job is queued in SLURM
- `running` - Job is currently executing
- `completed` - Job finished successfully
- `failed` - Job failed or was cancelled
- `unknown` - Status could not be determined
