from flask import Blueprint, jsonify, url_for, request

from business_data_api.tasks.krs_dokumenty_finansowe import KRSDokumentyFinansowe


bp_krs_df = Blueprint("bp_krs_df", __name__, url_prefix="/krs-df")

@bp_krs_df.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return jsonify({"status": "ok", "message": "KRS Dokumenty Finansowe is running"}), 200

@bp_krs_df.route("/get-document-names", mehtods=["GET"])
def get_document_list_for_krs()
    krs = request.args.get("krs")
    pass

    