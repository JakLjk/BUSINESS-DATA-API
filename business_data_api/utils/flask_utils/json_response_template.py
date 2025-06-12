from flask import jsonify


def compile_message(title: str,
                    message: str,
                    data: dict = None,
                    error: str = None) -> dict:
    response = {
        "title": title,
        "message": message,
        "data": data if data is not None else {},
        "error": error if error is not None else None
    }
    return jsonify(response)