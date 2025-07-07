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
    job = queue.enqueue(task_get_document_list, job_id, krs, job_id=job_id)
    log_api_krsdf.debug(f"Scraping task was added to queue. id:{job_id}")
    return APIResponse(
        status=ResponseStatus.ENQUEUED,
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
        f"\nStarting process of fetching result "
        f"\nof document names scraping process"
        f"\nJob id:{job_id}")
    redis_conn = request.app.state.redis
    try:
        logging.debug(f"Fetching information from job id: {job_id}")
        job = Job.fetch(job_id, connection=redis_conn)]
    except NoSuchJobError as e:
        error_message = (
            f"\nUnable to find job  that is supposed to"
            f"\nhave information about document names."
            f"\nJob id:{job_id}"
            f"\nError: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=404,
            detail=error_message
        )
    except InvalidJobOperation as e
        error_message = (
            f"\nUnable to find job  that is supposed to"
            f"\nhave information about document names."
            f"\nJob id:{job_id}"
            f"\nError: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=404,
            detail=error_message
        )
    except Exception as e:
        error_message = (
            f"\nUnknown error has occurred during process"
            f"\nof fetching job result with document names list"
            f"\nJob id:{job_id}"
            f"\nError: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=500,
            detail=error_message
        )
    if job.is_finished:
        log_api_krsdf.debug(
            f"Job has finished. Returning document names." 
            f"\n id:{job_id}")
        return APIResponse(
            status=ResponseStatus.SUCCESS,
            title="Document names retrieved",
            message="Requested document names were retrieved from host",
            data=job.result
        )
    elif job.is_failed:
        log_api_krsdf.debug(
            f"Job scraping document names list has failed." 
            f"\n id:{job_id}, \ne rror:{job.exc_info}")
        return APIResponse(
            status=ResponseStatus.FAILED,
            title="Scraping job failed"
            message="Job has failed during scraping process",
            data=job.exc_info
        )
    elif job.is_queued or job.is_started:
        # TODO add logic to cancel jobs that are running for too long
        log_api_krsdf.debug(f"Job is currently in progress. id:{job_id}")
        return APIResponse(
            status=ResponseStatus.PENDING,
            title="Job is in progress",
            message="Job is currently being handled by worker",
            data={
                "job_started_at":job.started_at,
                "job_queued_at":job.queued_at
            }
        )

@router.post(
    "/scrape-documents",
    summary=(
        "Scrape documents from the official KRS site"
        "by providing company KRS number and hash ids of documents to scrape"
        ),
    response_model=APIResponse)
async def scrape_documents(
    request: Request,
    krs: str = Query(..., 
                min_length=10,
                max_length=10),
    hash_ids:List[str] = Body(...)
    ):
    # TODO add logic that will insert record with pending status
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debut(f"Scraping {len(hash_ids)} documents for krs {krs}")
    queue = request.app.state.queues["KRSDF"]
    job_id = str(uuid.uuid4())
    log_api_krsdf.debug(f"Enqueuing job <task_scrape_krsdf_documents> id: {job_id}")
    job = queue.enqueue(task_scrape_krsdf_documents, job_id, krs, hash_ids, job_id=job_id)
    log_api_krsdf.debug(f"Scraping task added to queue. id" {job_id})
    return APIResponse(
        status=ResponseStatus.ENQUEUED,
        title="Scraping job for added to queue",
        message=f"Scraping of {len(hash_ids)} documents for {krs} was added to the queue",
        data={"job_id":job_id}
        )

@router.post(
    "/documents-scraping-status",
    summary=(
        "Check status of documents."
        "Documents can be already scraped, or in queue, or not being"
        "processed at all."
        ),
    response_model=APIResponse
    )
async def documents_scraping_status(
    request: Request,
    hash_ids:List[str] = Body(...)
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug(f"Opening PostgreSQL DB session")
    async with request.app.state.psql_asession() as session:
        log_api_krsdf.debug(f"Fetching information about existing document records from DB")
        query = select(RedisScrapingRegistry).where(RedisScrapingRegistry.hash_id.in_(hash_ids))
        results = await session.execute(query)
        rows = results.scalars().all()
    documents = {result.hash_id : result.job_status for result in rows}
    log_api_krsdf.debug(f"Returning information about {len(documents)} records that are in DB")
    return APIResponse(
        status=ResponseStatus.SUCCESS,
        title="Document scraping statuses",
        message="Returning information about documen statuses that are recorded in DB",
        data=documents
    )

@router.post(
    "/download-documents",
    summary=(
        "Download documents by providing their hash_ids",
        "Documents will be fetched from local PostgreSQL DB (If exists)"
        "And packed into ZIP file and then send as Streaming Response"
        "IF documements are not available in db, they won't be downloaded"
    ),
    response_model=StreamingResponse
    )
async def download_documents(
    request: Request,
    hash_ids: List[str] = Body(...)
    ):
    # TODO add logic for handling requests which request download documents that are 
    # not in database
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug(f"Opening PostgreSQL DB session")
    async with request.app.state.psql_asession() as session:
        log_api_krsdf.debug("Fetching information about records that are to be downloaded")
        query = select(ScrapedKrsDF).where(ScrapedKrsDF.hash_id.in_(hash_ids))
        results = await session.execute(query)
        rows = results.scalars().all()
    log_api_krsdf.debug("Creating zip stream and packing data into ZIP file")
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for row in rows:
            zip_file.writestr(row.document_content_save_name, row.document_content)
    zip_stream.seek(0)
    log_api_krsdf.debug(f"Returning zipped documents to the user")
    return StreamingResponse(
        zip_stream,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=documents.zip"}
    )




