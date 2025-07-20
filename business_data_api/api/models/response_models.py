from enum import Enum
from typing import List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, EmailStr, HttpUrl


class JobEnqueued(BaseModel):
    job_id: str
    job_status_url: str
    message: str
    
class JobStatus(BaseModel):
    job_id:str
    job_status:str
    job_enqueued_at:Optional[datetime]
    job_started_at:Optional[datetime]
    job_ended_at:Optional[datetime]
    job_result:Optional[Any]
    job_exc_info:Optional[str]

class DocumentInfo(BaseModel):
    document_name:str
    document_date_from:str
    document_date_to:str
    document_hash_id:str

class AvailableKRSDFDocuments(BaseModel):
    document_list:List[DocumentInfo]

class RequestHashIDs(BaseModel):
    hash_ids: List[str]

class CompanyInfoResponse(BaseModel):
    record_created_at: datetime
    full_name: str
    legal_form: str
    krs_number: str
    nip_number: str
    regon_number: str
    country: Optional[str] = None
    voivodeship: Optional[str] = None
    municipality: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    postal_number: Optional[str] = None
    street: Optional[str] = None
    house_number: Optional[str] = None
    email: Optional[EmailStr] = None
    webpage: Optional[HttpUrl] = None

    class Config:
        from_attributes = True
