from typing import Optional, Any, Dict

def compile_message(
    title: str,
    message: str,
    data: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None
) -> Dict[str, Any]:
    return {
        "title": title,
        "message": message,
        "data": data or {},
        "error": error
    }