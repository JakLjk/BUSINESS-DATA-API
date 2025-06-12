from flask import Blueprint, jsonify

bp_krs_api = Blueprint("bp_krs_api", __name__, url_prefix="/krs-api")

@bp_krs_api.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return jsonify({"status": "ok", "message": "KRS API is running"}), 200
