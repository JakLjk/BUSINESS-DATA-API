from pydantic import BaseModel
from pydantic.generics import GenericModel
from enum import Enum
from typing import Optional, Dict, Any, Union, List, TypeVar, Generic

from business_data_api.workers.tasks.status import JobTaskStatus

data_type = TypeVar("data_type")

class ResponseStatus(str, Enum):
    SUCCESS = "finished"
    PENDING = "pending"
    FAILED = "failed"
    UNKNOWN = "unknown"
    ENQUEUED = "enqueued"

class APIResponse(GenericModel, Generic(data_type)):
    status: ResponseStatus
    title: str
    message: str
    data: Optional[data_type] = None


class DocumentNamesData(BaseModel):
    documents: List

class DocumentScrapingStatusData(BaseModel):
    job_id:str
    hash_id_statuses:Dict[str, JobTaskStatus]

class JobEnqueuedData(BaseModel):
    job_id:str
    job_enqueued_at:str
    job_task:str
    job_variables:dict

class JobStatusData(BaseModel):
    job_id:str
    message=str
    job_task_status:Optional[JobTaskStatus]
    job_runtime_status:Optional[str]
    job_execution_info:Optional(str)
    job_enqueued_at:Optional[str]
    job_started_at:Optional[str]
    

class HashIdsRequest(BaseModel):
    hash_ids: List[str]