from dotenv import load_dotenv
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS, cross_origin
import subprocess
import io
import os
import json
import time
import paramiko
import re
import logging
import threading

load_dotenv()  # Load variables from .env file

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# Suppress paramiko DEBUG messages about dead connections (they're usually harmless)
logging.getLogger("paramiko.transport").setLevel(logging.WARNING)
# Configure CORS origins from environment variable or use defaults
# Read CORS origins from env and strip whitespace/trailing slashes
cors_origins_env = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,https://venerable-speculoos-b273da.netlify.app",
)
cors_origins = [
    origin.strip().rstrip("/")
    for origin in cors_origins_env.split(",")
    if origin.strip()
]

# Always include localhost:5173 for local development
if "http://localhost:5173" not in cors_origins:
    cors_origins.append("http://localhost:5173")

# Note: Flask-CORS doesn't support wildcard patterns, but we'll check for netlify.app in the OPTIONS handler

app.logger.debug(f"CORS origins configured: {cors_origins}")


# Helper function to check if origin should be allowed
def is_allowed_origin(origin):
    """Check if origin is in allowed list or is a Netlify/localhost domain"""
    if not origin:
        return False
    origin = origin.strip().rstrip("/")
    if origin in cors_origins:
        return True
    # Allow any Netlify domain
    if "netlify.app" in origin:
        return True
    # Allow localhost for development
    if "localhost" in origin or "127.0.0.1" in origin:
        return True
    return False


# Apply CORS globally with proper configuration.
# Pass a list of strings only; Flask-CORS fails if origins is a callable (e.g. in 404 handling).
# Dynamic Netlify/localhost is handled in after_request below.
CORS(
    app,
    origins=cors_origins,
    allow_headers=[
        "Content-Type",
        "ngrok-skip-browser-warning",
        "Authorization",
        "X-Requested-With",
    ],
    methods=["GET", "POST", "OPTIONS", "PUT", "DELETE"],
    supports_credentials=True,
    expose_headers=["Content-Type"],
    max_age=3600,  # Cache preflight requests for 1 hour
)

# Make cors_origins available to blueprints
app.config["CORS_ORIGINS"] = cors_origins


@app.after_request
def add_cors_for_allowed_origin(response):
    """Set CORS for origins allowed by is_allowed_origin but not in cors_origins (e.g. other Netlify URLs)."""
    origin = request.headers.get("Origin")
    if origin and is_allowed_origin(origin) and origin not in cors_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    return response


# Register blueprints
from blueprints import drn_bp

app.register_blueprint(drn_bp)


@app.route("/api/test-cors", methods=["GET", "POST", "OPTIONS"])
def test_cors():
    """Test endpoint to verify CORS is working"""
    if request.method == "OPTIONS":
        return "", 200

    return jsonify(
        {
            "status": "ok",
            "message": "CORS is working!",
            "origin": request.headers.get("Origin"),
            "cors_origins": cors_origins,
        }
    )


if __name__ == "__main__":
    app.logger.info(f"Starting Flask server on port 8000")
    app.logger.info(f"CORS origins: {cors_origins}")
    app.run(host="0.0.0.0", port=8000, debug=True)
