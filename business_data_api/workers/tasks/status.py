from enum import Enum

class JobTaskStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"