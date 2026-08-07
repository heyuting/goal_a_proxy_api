"""Auth / Duo MFA bridge endpoints for the web UI."""
from flask import Blueprint, jsonify, request

from utils.ssh import mfa_bridge

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/api/auth/mfa-status", methods=["GET", "OPTIONS"])
def mfa_status():
    if request.method == "OPTIONS":
        return "", 200
    return jsonify(mfa_bridge.snapshot())


@auth_bp.route("/api/auth/mfa-response", methods=["POST", "OPTIONS"])
def mfa_response():
    if request.method == "OPTIONS":
        return "", 200

    data = request.get_json(silent=True) or {}
    choice = data.get("choice", data.get("response", ""))
    auth_id = data.get("auth_id")

    ok, error = mfa_bridge.submit_response(response=choice, auth_id=auth_id)
    if not ok:
        return jsonify({"ok": False, "error": error, **mfa_bridge.snapshot()}), 409

    return jsonify(
        {
            "ok": True,
            **mfa_bridge.snapshot(),
            "status": "accepted",
            "message": "MFA response submitted",
        }
    )
