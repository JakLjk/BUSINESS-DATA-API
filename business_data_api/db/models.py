from sqlalchemy import Column, String, Text, LargeBinary, TIMESTAMP, Integer
from sqlalchemy.sql import func
from sqlalchemy import Enum as PSQLEnum
from enum import Enum

from . import Base

class ScrapingStatus(Enum):
    PENDING = "pending"
    FINISHED = "finished"
    FAILED = "failed"


class ScrapedKrsDF(Base):
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

class RedisScrapingRegistry(Base):
    __tablename__ = "redis_scraping_registry"
    hash_id = Column(String, primary_key=True)
    job_id = Column(String)
    job_status = Column(PSQLEnum(ScrapingStatus), 
                            nullable=False,
                            default=ScrapingStatus.PENDING)
    scraping_error_message = Column(Text)
    record_created_at = Column(TIMESTAMP, server_default=func.now())
    record_updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

class ScrapingCommissions:
    id = Column(Integer, primary_key=True)
    job_id = Column(String)
    hash_id = Column(String)
    job_status = Column(PSQLEnum(ScrapingStatus), 
                            nullable=False,
                            default=ScrapingStatus.PENDING)
    message = Column(Text)
    record_created_at = Column(TIMESTAMP, server_default=func.now())
    record_updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now()) 