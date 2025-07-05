import os
import uuid
import io
import zipfile

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from typing import Literal, List
from urllib.parse import urlencode
from rq.job import Job
from rq.exceptions import NoSuchJobError, InvalidJobOperation
from sqlalchemy import select
from dotenv import load_dotenv


from business_data_api.utils.response_templates.default_response import APIResponse
from business_data_api.workers.tasks.task_scrape_krsdf_document_list import task_get_document_list
from business_data_api.workers.tasks.task_scrape_krsdf_documents import task_scrape_krsdf_documents
from business_data_api.db.models import RedisScrapingRegistry, ScrapedKrsDF
from business_data_api.utils.logger import setup_logger

router = APIRouter()

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")

log_api_krsdf = setup_logger(
    logger_name="fast_api_route_krsdf",
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )

# TODO 
# Set repsonses to pydantic
# Finish setting up proper logging
# Use HTTP error codes for unexpected failures (like scraping exceptions). For expected or business-level errors (e.g., “no data found”), you can return a structured response with a failed status in your Pydantic model.
@router.get(
    "/health", 
    summary="API KRS-DF health check",
    response_model=APIResponse)
async def health(
    request: Request
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    return APIResponse(
        status=ResponseStatus.SUCCESS,
        title="Health OK",
        message=""
    )

@router.get(
    "/get-document-names/{krs}",
    summary="Get names of available documents for KRS number",
    response_model=APIResponse)
async def get_document_names(
    request: Request,
    krs: str
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug(f"Starting process of getting document names for krs {krs}")
    queue = request.app.state.queues["KRSDF"]
    job_id = str(uuid.uuid4())
    log_api_krsdf.debug(f"Enqueuing job <task_get_document_list> id:{job_id}")
    job = queue.enqueue(task_get_document_list, krs, job_id=job_id)
    log_api_krsdf.debug(f"Scraping task was added to queue. id:{job_id}")
    return APIResponse(
        status=MessageStatus.SUCCESS,
        title="Scraping task was added to queue",
        message=f"go to /get-document-names-result/{job_id} to see the results",
        data={"job_id":job_id}
    )

@router.get(
    "/get-document-names-result/{job_id}",
    name="get_document_names_result",
    summary= (
        "Get document names that were scraped"
        "by providing job id that was responsible for"
        "the scraping process"
        ),
    response_model=APIResponse)
async def get_document_names_result(
    request: Request,
    job_id: str
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug(
        f"Starting process of fetching result "
        f"of document names scraping process"
        f"Job id:{job_id}")
    redis_conn = request.app.state.redis
    try:
        logging.debug(f"Fetching information from job id: {job_id}")
        job = Job.fetch(job_id, connection=redis_conn)]
    except NoSuchJobError as e:
        error_message = (
            f"Unable to find job  that is supposed to"
            f"have information about document names."
            f"Job id:{job_id}"
            f"Error: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=404,
            detail=error_message
        )
    except InvalidJobOperation as e
        error_message = (
            f"Unable to find job  that is supposed to"
            f"have information about document names."
            f"Job id:{job_id}"
            f"Error: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=404,
            detail=error_message
        )
    except Exception as e:
        error_message = (
            f"Unknown error has occurred during process"
            f"of fetching job result with document names list"
            f"Job id:{job_id}"
            f"Error: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=500,
            detail=error_message
        )
    if job.is_finished:
        log_api_krsdf.debug(f"Job has finished. Returning document names. id:{job_id}")
        return APIResponse(
            status=ResponseStatus.SUCCESS,
            title="Document names retrieved",
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



