# from typing import Optional, Any, Dict
# from enum import Enum

# class MessageStatus(str, Enum):
#     FINISHED = "finished"
#     PENDING = "pending"
#     FAILED = "failed"
#     UNKNOWN = "unknown"

# def compile_message(
#     status: MessageStatus,
#     title: str,
#     message: str,
#     data: Optional[Dict[str, Any]] = None,
#     error: Optional[str] = None
# ) -> Dict[str, Any]:
#     return {
#         "status": status,
#         "title": title,
#         "message": message,
#         "data": data or {},
#         "error": error
#     }


from pydantic import BaseModel
from enum import Enum

class ResponseStatus(str, Enum):
    SUCCESS = "finished"
    PENDING = "pending"
    FAILED = "failed"
    UNKNOWN = "unknown"
    ENQUEUED = "enqueued"

class APIResponse(BaseModel):
    status: ResponseStatus
    title: str
    message: str
    data: Optional[Dict[str, any]] = {}
    