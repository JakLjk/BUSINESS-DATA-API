from sqlalchemy import (
    Column, 
    String, 
    Unicode, 
    Text, 
    LargeBinary, 
    TIMESTAMP, 
    Integer,
    DateTime,
    Date,
    Boolean)
from sqlalchemy.sql import func
from sqlalchemy import Enum as PSQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from enum import Enum

from . import Base


## Models populated by KRS DF
class KRSDFDocuments(Base):
    __tablename__ = "krs_df_documents"
    hash_id = Column(String, primary_key=True)
    krs_number = Column(String)
    document_internal_id = Column(String)
    document_type = Column(String)
    document_name = Column(String)
    document_date_from = Column(String)
    document_date_to = Column(String)
    document_status = Column(String)
    document_content_save_name = Column(String)
    document_content_file_extension = Column(String)
    document_content = Column(LargeBinary)
    record_created_at = Column(TIMESTAMP, server_default=func.now())
    record_updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    
## MODELS POPULATED BY KRS API
# TODO ADD FOREIGN KEYS
class RawKSRAPIFullExtract(Base):
    __tablename__ = "raw_krs_api_full_extract"
    id = Column(Integer, primary_key=True, index=True)
    record_created_at = Column(
        DateTime(timezone=True),
        server_default=func.now())
    is_current = Column(Boolean, default=False)
    krs_number = Column(String(10), nullable=False)
    raw_data = Column(JSONB, nullable=False)
    
    
# class CompanyInfo(Base):
#     __tablename__ = "company_info"
#     id = Column(Integer, primary_key=True, index=True)
#     record_created_at = Column(
#         DateTime(timezone=True),
#         server_default=func.now())
#     is_current = Column(Boolean, default=False)
#     full_name = Column(String(250), nullable=False)
#     legal_form = Column(String(100), nullable=False)
#     krs_number = Column(String(14), nullable=False)
#     nip_number = Column(String(10), nullable=False)
#     regon_number = Column(String(14), nullable=False)
#     country = Column(String(100), nullable=True)
#     voivodeship = Column(String(100), nullable=True)
#     municipality = Column(String(100), nullable=True)
#     county = Column(String(100), nullable=True)
#     city = Column(String(100), nullable=True)
#     postal_number = Column(String(6), nullable=True)
#     street = Column(String(100), nullable=True)
#     house_number = Column(String(20), nullable=True)
#     email = Column(String(320), nullable=True)
#     webpage = Column(String(2083), nullable=True) 


# class CompanyInfoDetails(Base):
#     __tablename__ = "company_info_details"
#     id = Column(Integer, primary_key=True, index=True)
#     is_current = Column(Boolean, default=False)
#     record_created_at = Column(
#         DateTime(timezone=True),
#         server_default=func.now())
#     registered_at_krs_system_date = Column(Date)


