from enum import Enum

class JobTaskStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"

class DocumentScrapingStatus(str, Enum):
    SUCCESS = "scraped"
    FAILED = "failed"