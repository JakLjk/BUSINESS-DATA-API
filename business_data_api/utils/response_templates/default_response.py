
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
    