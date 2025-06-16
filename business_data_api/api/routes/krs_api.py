from flask import Blueprint, jsonify, url_for, request

from business_data_api.utils.flask_utils.json_response_template import compile_message
from business_data_api.tasks.krs_api.get_krs_api import KRSApi
from business_data_api.tasks.exceptions import InvalidParameterException, EntityNotFoundException


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
            },
            "/get-historia-zmian": {
                "method": "GET",
                "description": "Returns the history of changes for a given KRS number.",
                "query_parameters": {
                    "dzien": "Required. Date in the format YYYY-MM-DD (string).",
                    "godzinaOd": "Required. Start hour in the format HH (string).",
                    "godzinaDo": "Required. End hour in the format HH (string)."
                },
                "example": "/get-historia-zmian?dzien=2023-10-01&godzinaOd=08&godzinaDo=18"
            }
        },
        "note": "All responses are returned in JSON format."

    }
    return compile_message(
        "KRS API Documentation",
        "This API provides access to Krajowy Rejestr Sądowy (KRS) data.",
        data), 200


@bp_krs_api.route("/get-odpis", methods=["GET"])
def get_odpis():
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
            "Please refer to the documentation for required parameters.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            }), 400

    # Fetching the KRS extract and handling exceptions
    try:
        fetched_odpis = KRSApi().get_odpis(krs, rejestr, typ_odpisu)
        return compile_message(
            "KRS Extract Retrieved",
            "The requested KRS extract has been successfully retrieved.",
            fetched_odpis), 200
    except InvalidParameterException as e:
        return compile_message(
            "Invalid parameters",
            "One or more parameters are invalid. Please check the provided KRS number and register type.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },
            str(e)), 400
    except EntityNotFoundException as e:
        return compile_message(
            "Entity not found",
            "Entity could not be found for the provided KRS number and register type.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },
            str(e)), 404
    except Exception as e:
        return compile_message(
            "Internal server error",
            "An unexpected error occurred while processing your request.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },
            str(e)), 500


@bp_krs_api.route("/get-historia-zmian", methods=["GET"])
def get_historia_zmian():
    """
    Endpoint to get the history of changes for a given KRS number.
    """

    # Loading parameters from the request
    dzien = request.args.get("dzien", type=str)
    godzina_od = request.args.get("godzinaOd", type=str)
    godzina_do = request.args.get("godzinaDo", type=str)

    # Check if parameters are provided
    if not dzien or not godzina_od or not godzina_do:
        return compile_message(
            "Missing required parameters",
            "Please refer to the documentation for required parameters.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },), 400
    
    # Fetching the history of changes and handling exceptions
    try:
        historia_zmian = KRSApi().get_historia_zmian(dzien, godzina_od, godzina_do)
        return compile_message(
            "History of Changes Retrieved",
            "The requested history of changes has been successfully retrieved.",
            historia_zmian), 200
    except InvalidParameterException as e:
        return compile_message(
            "Invalid parameters",
            "One or more parameters are invalid. Please check the provided date and time range.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },
            str(e)), 400
    except EntityNotFoundException as e:
        return compile_message(
            "Entity not found",
            "No changes found for the provided date and time range.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },
            str(e)), 404
    except Exception as e:
        return compile_message(
            "Internal server error",
            "An unexpected error occurred while processing your request.",
            {
                "docs": request.url_root.rstrip('/') + url_for('bp_krs_api.docs')
            },
            str(e)), 500
