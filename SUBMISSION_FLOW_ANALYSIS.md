# Job Submission Flow Analysis

## Current Flow

### Backend Submission (`/api/drn/full-pipeline` POST)

1. **Immediate Response (Synchronous)**

   - Validates input parameters
   - Generates unique `job_id` (e.g., `drn_full_1765219425_3875`)
   - Creates job entry in `JOB_STATUS_CACHE` with:
     - `status: "submitting"`
     - `grace_job_id: None`
     - `submitted_at: time.time()`
   - Starts background thread
   - **Returns immediately** with:
     ```json
     {
       "job_id": "drn_full_...",
       "grace_job_id": null,
       "status": "submitting",
       "message": "DRN full pipeline job is being submitted..."
     }
     ```

2. **Background Thread (Asynchronous)**
   - Connects to Grace HPC via SSH
   - Creates job folder: `/home/{GRACE_USER}/project_pi_par35/yhs5/DRN/jobs/{job_id}`
   - Creates output directory
   - Writes parameters.json
   - Creates SLURM job script
   - Makes script executable
   - Submits to SLURM: `sbatch {script_path}`
   - **On Success**: Updates cache with:
     - `grace_job_id: <SLURM_JOB_ID>`
     - `status: "submitted"`
   - **On Failure**: Updates cache with:
     - `status: "failed"`
     - `error: "<error message>"`

### Frontend Submission (`submitFullPipeline`)

1. **Submit Request**

   - Sends POST to `/api/drn/full-pipeline`
   - Receives `job_id` and `status: "submitting"`
   - Saves to localStorage
   - Calls `pollFullPipelineStatus(job_id)`

2. **Status Polling (`pollFullPipelineStatus`)**
   - Polls `/api/drn/full-pipeline/{job_id}/status`
   - Polls every 30 seconds (or based on status)
   - Updates UI based on status changes

### Backend Status Check (`/api/drn/full-pipeline/<job_id>/status` GET)

**Current Logic:**

1. Gets job from `JOB_STATUS_CACHE`
2. If `grace_job_id` is `None`:

   - Checks if job folder exists on Grace
   - **If folder NOT found**: Returns 404
   - **If folder found but no job_id in logs**:
     - Checks completion status
     - **If completed**: Returns completed
     - **If not completed**: Returns **500 ERROR** ❌

3. If `grace_job_id` exists:
   - Checks SLURM status with `squeue`
   - Returns appropriate status

## Issues Identified

### Issue 1: 500 Error During Submission

**Problem:**
When `grace_job_id` is `None` and the job folder exists but no SLURM job_id is found in logs, the endpoint returns a 500 error. This happens because:

1. Background thread creates the folder
2. Background thread writes the script
3. Background thread submits to SLURM (takes time)
4. Frontend polls status before SLURM submission completes
5. Status endpoint finds folder but no job_id → returns 500

**Expected Behavior:**

- If folder exists but no job_id found AND submission started < 5 minutes ago → return `"submitting"`
- If folder exists but no job_id found AND submission started > 5 minutes ago → return error

### Issue 2: No Error Handling for SSH Failures

**Problem:**
Status endpoint doesn't handle SSH connection failures gracefully. If SSH fails, it will crash with 500 error.

**Expected Behavior:**

- Try to connect to SSH
- If connection fails, return appropriate error message
- Don't crash the endpoint

### Issue 3: Missing Status Check for "submitting" State

**Problem:**
When `grace_job_id` is `None`, the code tries to recover it, but if recovery fails and it's still within submission window, it should return "submitting" status instead of error.

## Recommended Fixes

### Fix 1: Handle "Submitting" State Properly

In status endpoint, when `grace_job_id` is `None`:

```python
if not grace_job_id:
    # Check submission time
    submitted_at = job_info.get("submitted_at", time.time())
    time_elapsed = time.time() - submitted_at

    # If still within submission window (< 5 minutes)
    if time_elapsed < 300:
        # Check if folder exists
        if folder_exists == "not_found":
            # Still submitting, folder not created yet
            return jsonify({
                "job_id": job_id,
                "status": "submitting",
                "message": "Job is being submitted..."
            })
        elif folder_exists == "exists":
            # Folder exists but no job_id yet - still submitting
            return jsonify({
                "job_id": job_id,
                "status": "submitting",
                "message": "Job is being submitted..."
            })
    else:
        # Submission timeout - check for errors
        if folder_exists == "not_found":
            return jsonify({
                "job_id": job_id,
                "status": "failed",
                "error": "Job submission timed out"
            }), 500
```

### Fix 2: Add Error Handling for SSH

```python
try:
    ssh = get_ssh_connection()
except Exception as ssh_error:
    app.logger.error(f"SSH connection failed: {str(ssh_error)}")
    return jsonify({
        "job_id": job_id,
        "status": "unknown",
        "error": f"Failed to connect to Grace HPC: {str(ssh_error)}"
    }), 500
```

### Fix 3: Check Cache for Submission Errors

Before checking Grace, check if background thread reported an error:

```python
job_info = JOB_STATUS_CACHE.get(job_id, {})
if job_info.get("status") == "failed":
    return jsonify({
        "job_id": job_id,
        "status": "failed",
        "error": job_info.get("error", "Job submission failed")
    }), 500
```

## Summary

The main issue is that the status endpoint doesn't properly handle the "submitting" state when `grace_job_id` is `None`. It should:

1. Check how long it's been since submission started
2. If < 5 minutes, return "submitting" status (even if folder exists but no job_id)
3. If > 5 minutes, check for errors or return timeout
4. Only return 500/404 errors for actual failures, not for jobs still being submitted
