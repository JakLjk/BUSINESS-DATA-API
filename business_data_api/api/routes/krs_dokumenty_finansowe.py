from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.requests import Request
from typing import Literal, List
from urllib.parse import urlencode
from rq.job import Job
from rq.exceptions import NoSuchJobError
from sqlalchemy import select
from dotenv import load_dotenv
import os
import uuid

from business_data_api.utils.dict_response_template import compile_message
from business_data_api.worker.tasks.task_scrape_krsdf_document_list import task_get_document_list
from business_data_api.worker.tasks.task_scrape_krsdf_documents import task_scrape_krsdf_documents
from business_data_api.db.models import RedisScrapingRegistry

router = APIRouter()
load_dotenv()

@router.get("/health", summary="API KRS-DF health check")
async def health():
    return {"status": "ok", "message": "API is running"}

@router.get("/get-document-names", summary="Get names of available documents for KRS number")
async def get_document_names(
    request: Request,
    krs: str = Query(..., 
                    min_length=10,
                    max_length=10)
    ):
    queue = request.app.state.queues["KRSDF"]
    job_id = str(uuid.uuid4())
    job = queue.enqueue(task_get_document_list, krs, job_id=job_id)
    response_url = f"{str(request.url_for("get_document_names_result"))}?{urlencode({"job_id":job_id})}"
    return compile_message(
        "Task was added to queue",
        "",
        response_url,
    )

@router.get("/get-document-names-result",
            name="get_document_names_result")
async def get_document_names_result(
    request: Request,
    job_id: str = Query(...)
    ):
    redis_conn = request.app.state.redis
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception as e:
        raise HTTPException(
                            status_code=404, 
                            detail=f"Job not found: {str(e)}")
    if job.is_finished:
        return compile_message(
            "Documents names retrieved",
            "Requested document names were retrieved from host",
            job.result
        ) 
    elif job.is_failed:
        return compile_message(
            "Job has failed",
            "The job fetching document list has failed",
            "",
            job.exc_info
        ) 
    elif job.is_queued or job_is_started:
        return compile_message(
            "Job in progress",
            "Job is currently being handled by worker",
        )
    else:
        return compile_message(
            "Job status unknown",
            "Job status is unknown"
        )

@router.post("/get-download-documents", summary="Get specified documents for KRS")
async def get_documents(
    request: Request,
    krs: str = Query(..., 
                    min_length=10,
                    max_length=10),
    hash_ids: List[str] = Query(...)
):
    pass
@router.get("/get-download-document-status")
async def document_status(
    request: Request,
    krs: str = Query(..., 
                    min_length=10,
                    max_length=10),
    hash_ids: List[str] = Query(...)
    ):
    pass

@router.post("/scrape-documents")
async def scrape_documents(
    request: Request,
    hash_ids:List[str] = Body(...)
    ):
    queue = request.app.state.queues["KRSDF"]
    job_id = str(uuid.uuid4())
    job = queue.enqueue(task_scrape_krsdf_documents, job_id, krs, hash_ids, job_id=job_id)

@router.post("/download-documents")
async def download_documents():
    pass

@router.post("/documents-scraping-status")
async def documents_scraping_status(
    request: Request,
    hash_ids:List[str] = Body(...)
    ):
    session = request.app.state.psql_asession()
    results = (
            session.query(RedisScrapingRegistry)
            .filter(RedisScrapingRegistry.hash_id.in_(hash_ids))
            .all()
            )
    return {result.hash_id : result.job_status for result in results}



