from flask import Blueprint, jsonify, url_for, request

from business_data_api.utils.flask_utils.json_response_template import compile_message
from business_data_api.tasks.krs_api.get_krs_api import KRSApi

bp_krs_api = Blueprint("bp_krs_api", __name__, url_prefix="/krs-api")


@bp_krs_api.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return jsonify({"status": "ok", "message": "KRS API is running"}), 200

@bp_krs_api.route("/docs", methods=["GET"])    
def docs():
    """
    Endpoint to retrieve the documentation for the KRS API.
    """
    data = {
        "endpoints": {
            "/get-odpis": {
                "method": "GET",
                "description": "Returns the current KRS extract for a given entity.",
                "query_parameters": {
                    "krs": "Required. 10-digit KRS number (string).",
                    "rejestr": "Required. 'P' for business, 'S' for association.",
                    "typ_odpisu": "Optional. Type of extract [aktualny | pelny], default is 'aktualny'."
                },
                "example": "/get-odpis?krs=0000123456&rejestr=P&typ-odpisu=aktualny"
            }
        },
        "note": "All responses are returned in JSON format."

    }
    return compile_message(
        "KRS API Documentation",
        "This API provides access to Krajowy Rejestr Sądowy (KRS) data.",
        data), 200


@bp_krs_api.route("/get-odpis", methods=["GET"])
def get_odpis_aktualny():
    """
    Endpoint to get the current KRS extract.
    """
    # Loading parameters from the request
    krs = request.args.get("krs", type=str)
    rejestr = request.args.get("rejestr", type=str)
    typ_odpisu = request.args.get("typ_odpisu", type=str, default="aktualny")

    # Check if parameters are provided
    if not krs or not rejestr:
        return compile_message(
            "Missing required parameters",
            "The 'krs' and 'rejestr' parameters are required.",
            {
                "arguments": {
                    "krs": "10-digit KRS number (string)",
                    "rejestr": "'P' for business, 'S' for association"
                },
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            }), 400
    # Fetch data from KRS API and handle exceptions
    fetched_odpis_aktualny = KRSApi().get_odpis_aktualny(krs, rejestr)
    fetched_odpis_pelny = KRSApi().get_odpis_pelny(krs, rejestr)


    return "PASS", 200
    

@bp_krs_api.route("/get-historia-zmian", methods=["GET"])
def get_historia_zmian():
    """
    Endpoint to get the history of changes for a given KRS number.
    """
    return None
