from flask import Blueprint, jsonify

bp_root = Blueprint("bp_root", __name__, url_prefix="/")

@bp_root.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return jsonify({"status": "ok", "message": "Server is running"}), 200
