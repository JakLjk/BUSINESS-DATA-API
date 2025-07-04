from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Request, Body
from fastapi.responses import FileResponse, StreamingResponse
from typing import Literal, List
from urllib.parse import urlencode
from rq.job import Job
from rq.exceptions import NoSuchJobError
from sqlalchemy import select
from dotenv import load_dotenv
import os
import uuid
import io
import zipfile

from business_data_api.utils.dict_response_template import compile_message, MessageStatus
from business_data_api.workers.tasks.task_scrape_krsdf_document_list import task_get_document_list
from business_data_api.workers.tasks.task_scrape_krsdf_documents import task_scrape_krsdf_documents
from business_data_api.db.models import RedisScrapingRegistry, ScrapedKrsDF

router = APIRouter()
load_dotenv()

@router.get("/health", summary="API KRS-DF health check")
async def health():
    return {"status": "ok", "message": "API is running"}

@router.get("/get-document-names/{krs}", summary="Get names of available documents for KRS number")
async def get_document_names(
    request: Request,
    krs: str
    ):
    queue = request.app.state.queues["KRSDF"]
    job_id = str(uuid.uuid4())
    job = queue.enqueue(task_get_document_list, krs, job_id=job_id)
    return compile_message(
        MessageStatus.FINISHED,
        "Task was added to queue",
        "",
        {"job_id":job_id},
    )

@router.get("/get-document-names-result/{job_id}",
            name="get_document_names_result")
async def get_document_names_result(
    request: Request,
    job_id: str
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
            MessageStatus.FINISHED,
            "Documents names retrieved",
            "Requested document names were retrieved from host",
            job.result
        ) 
    elif job.is_failed:
        return compile_message(
            MessageStatus.FAILED,
            "Job has failed",
            "The job fetching document list has failed",
            "",
            job.exc_info
        ) 
    elif job.is_queued or job.is_started:
        return compile_message(
            MessageStatus.PENDING,
            "Job in progress",
            "Job is currently being handled by worker",
        )
    else:
        return compile_message(
            MessageStatus.UNKNOWN,
            "Job status unknown",
            "Job status is unknown"
        )

@router.post("/scrape-documents")
async def scrape_documents(
    request: Request,
    krs: str = Query(..., 
                min_length=10,
                max_length=10),
    hash_ids:List[str] = Body(...)
    ):
    queue = request.app.state.queues["KRSDF"]
    job_id = str(uuid.uuid4())
    job = queue.enqueue(task_scrape_krsdf_documents, job_id, krs, hash_ids, job_id=job_id)
    return compile_message(
        MessageStatus.FINISHED,
        "Documents added to the scraping queue",
        hash_ids
        )

@router.post("/download-documents")
async def download_documents(
    request: Request,
    hash_ids: List[str] = Body(...)
    ):
    print("XXXXXX")
    print(hash_ids)
    async with request.app.state.psql_asession() as session:
        query = select(ScrapedKrsDF).where(ScrapedKrsDF.hash_id.in_(hash_ids))
        results = await session.execute(query)
        rows = results.scalars().all()
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for row in rows:
            zip_file.writestr(row.document_content_save_name, row.document_content)
    zip_stream.seek(0)
    return StreamingResponse(
        zip_stream,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=documents.zip"}
    )

@router.post("/documents-scraping-status")
async def documents_scraping_status(
    request: Request,
    hash_ids:List[str] = Body(...)
    ):
    # print("YYYYYYY")
    # print(hash_ids)
    async with request.app.state.psql_asession() as session:
        query = select(RedisScrapingRegistry).where(RedisScrapingRegistry.hash_id.in_(hash_ids))
        results = await session.execute(query)
        rows = results.scalars().all()
    documents = {result.hash_id : result.job_status for result in rows}
    return compile_message(
        MessageStatus.FINISHED,
        "Document scraping status",
        "Displaying each document scraping status",
        documents
    )



