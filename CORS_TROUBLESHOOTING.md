# CORS Troubleshooting Guide

## The Error

```
Access to fetch at 'https://ed570d45c17f.ngrok-free.app/api/drn/site-selection'
from origin 'http://localhost:5173' has been blocked by CORS policy
```

This means the backend is not allowing requests from `http://localhost:5173`.

## Quick Fix

### 1. Restart the Flask Backend

After updating CORS configuration, **restart the Flask server**:

```bash
# Stop the current server (Ctrl+C)
# Then restart:
cd /Users/yutingsmeglin/Desktop/YaleWork/goal_a_proxy_api
python app.py
```

### 2. Verify CORS Configuration

Check that the backend logs show:

```
CORS origins: ['http://localhost:5173', ...]
```

### 3. Test CORS

Test if CORS is working:

```bash
# From your browser console or terminal:
curl -X GET https://ed570d45c17f.ngrok-free.app/api/test-cors \
  -H "Origin: http://localhost:5173" \
  -v
```

You should see `Access-Control-Allow-Origin: http://localhost:5173` in the response headers.

### 4. Check Frontend Configuration

Make sure your frontend `.env` file has:

```env
VITE_API_BASE_URL=https://ed570d45c17f.ngrok-free.app
```

## How Location Data is Sent

The frontend sends location data like this:

```javascript
// In DRNconfig.jsx, submitSiteSelection():
body: JSON.stringify({
  locations: selectedLocations.map((loc) => ({ lat: loc.lat, lon: loc.lng })),
});
```

Example payload:

```json
{
  "locations": [
    { "lat": 37.0, "lon": -78.0 },
    { "lat": 36.5, "lon": -77.5 }
  ]
}
```

## Backend Processing

1. **Receives request** at `/api/drn/site-selection`
2. **Validates** coordinates (lat/lon ranges, format)
3. **Creates job folder** on Grace: `/home/yhs5/project/DRN/jobs/{job_id}/`
4. **Writes coordinates** to `coords.json` in job folder
5. **Creates SLURM script** that:
   - Runs Python script with coordinates
   - Converts shapefiles to GeoJSON
6. **Submits SLURM job** to Grace
7. **Returns job_id** to frontend

## Debugging Steps

### Step 1: Check Backend Logs

Look for these log messages:

```
Received request for site selection from origin: http://localhost:5173
Request payload: {'locations': [{'lat': 37.0, 'lon': -78.0}]}
```

If you don't see these, the request isn't reaching the backend (CORS blocking).

### Step 2: Test with curl

```bash
curl -X POST https://ed570d45c17f.ngrok-free.app/api/drn/site-selection \
  -H "Content-Type: application/json" \
  -H "Origin: http://localhost:5173" \
  -H "ngrok-skip-browser-warning: true" \
  -d '{"locations": [{"lat": 37.0, "lon": -78.0}]}' \
  -v
```

Check for:

- `Access-Control-Allow-Origin: http://localhost:5173` in response headers
- Successful response with `job_id`

### Step 3: Check Browser Network Tab

1. Open browser DevTools → Network tab
2. Try submitting site selection
3. Look for the request to `/api/drn/site-selection`
4. Check:
   - **Request Headers**: Should include `Origin: http://localhost:5173`
   - **Response Headers**: Should include `Access-Control-Allow-Origin: http://localhost:5173`
   - **Status**: Should be 200 (not CORS error)

## Common Issues

### Issue 1: Backend Not Restarted

**Solution**: Restart Flask server after CORS changes

### Issue 2: ngrok Interfering

**Solution**: Make sure ngrok is forwarding correctly:

```bash
ngrok http 8000
```

### Issue 3: Origin Mismatch

**Solution**: Ensure CORS origins list includes exact origin:

- `http://localhost:5173` (not `http://localhost:5173/`)

### Issue 4: Preflight OPTIONS Failing

**Solution**: Flask-CORS should handle this automatically, but explicit OPTIONS handler was added

## Verification

After fixing, you should see in browser console:

- Request succeeds
- Response contains `job_id`
- No CORS errors

And in backend logs:

- "Received request for site selection from origin: http://localhost:5173"
- "Request payload: {'locations': [...]}"
