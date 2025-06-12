from flask import Blueprint, jsonify

bp_krs_df = Blueprint("bp_krs_df", __name__, url_prefix="/krs-df")

@bp_krs_df.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return jsonify({"status": "ok", "message": "KRS Dokumenty Finansowe is running"}), 200

    