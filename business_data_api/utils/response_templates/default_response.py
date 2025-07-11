from pydantic import BaseModel
from enum import Enum
from typing import Optional, Dict, Any, Union, List, TypeVar, Generic
from datetime import datetime

from business_data_api.workers.tasks.status import JobTaskStatus
from business_data_api.db.models import ScrapingStatus
data_type = TypeVar("data_type")

class ResponseStatus(str, Enum):
    SUCCESS = "finished"
    PENDING = "pending"
    FAILED = "failed"
    UNKNOWN = "unknown"

class APIResponse(BaseModel, Generic[data_type]):
    status: ResponseStatus
    title: str
    message: str
    data: Optional[data_type] = None

class DocumentNamesData(BaseModel):
    documents: List

class DocumentNamesScrapingStatusData(BaseModel):
    job_id:str
    job_task_status:Optional[JobTaskStatus]
    job_runtime_status:Optional[str]
    job_execution_info:Optional[str]
    job_enqueued_at:Optional[datetime]
    job_started_at:Optional[datetime]

class DocumentScrapingStatusData(BaseModel):
    job_id:str
    job_runtime_status:Optional[str]
    job_execution_info:Optional[str]
    job_enqueued_at:Optional[datetime]
    job_started_at:Optional[datetime]
    hash_id_statuses:Dict[str, ScrapingStatus]

class JobEnqueuedData(BaseModel):
    job_id:str
    job_enqueued_at:datetime
    job_task:str
    job_variables:dict

class HashIdsRequest(BaseModel):
    hash_ids: List[str]