from sqlalchemy import Column, String, Text, LargeBinary, TIMESTAMP
from sqlalchemy.sql import func
from . import Base

class ScrapedKrsDF(Base):
    __tablename__ = "scraped_krs_df"
    hash_id = Column(String, primary_key=True)
    krs = Column(String)
    document_internal_id = Column(Integer)
    document_type = Column(String)
    document_name = Column(String)
    document_date_from = Column(String)
    document_date_to = Column(String)
    document_status = Column(String)
    content_type = Column(String)
    content_content = Column(LargeBinary)
    status = Column(String, nullable=False, default="pending")
    error_message = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now()),
    file_extension = Column(String),
    save_name = Column(String)
