
# Goal A Proxy API

This API submits ATS, DRN, and SCEPTER jobs to Grace HPC via SSH.

## API Endpoints

- **POST /run-job**  
Submit a model run to Grace. Payload:

```
{
  "model": "ats",  // or "drn" or "scepter"
  "parameters": { /* your model parameters */ }
}
```

- **POST /check-job-status**  
Check the job status. Payload:

```
{
  "job_id": "123456",
  "job_folder": "/scratch/your_username/goal_a_jobs/ats-<timestamp>"
}
```

## Deployment

Deploy to Render using this repo. Set these environment variables in Render:

- `GRACE_USER`
- `GRACE_HOST` (e.g., grace.ycrc.yale.edu)
- `SSH_PRIVATE_KEY` (private key with \n newlines)
