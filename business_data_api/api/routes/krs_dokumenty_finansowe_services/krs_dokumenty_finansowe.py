import os
import uuid
import io
import zipfile

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from rq.job import Job
from rq.exceptions import NoSuchJobError, InvalidJobOperation
from sqlalchemy import select
from dotenv import load_dotenv
from business_data_api.utils.logger import setup_logger
from business_data_api.db.models import KRSDFDocuments
from business_data_api.workers.tasks.scraping_krs_df.scrape_documents import task_scrape_documents
from business_data_api.api.models import(
    JobEnqueued,
    JobStatus,
    AvailableKRSDFDocuments,
    DocumentInfo,
    RequestHashIDs,
)

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
log = setup_logger(
    logger_name="route_krs_df",
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url)

router = APIRouter()


@router.get(
    "/health", 
    summary="KRS DF route health check")
async def health():
    log.info(f"Returning information about route health status")
    return "Health ok.", 200

@router.get(
        "/update-document-list/{krs}",
        summary=(
                "Use this endpoint to update document list available locally. "
                "By providing krs number, this function can get all the business"
                "documents from the official KRS Registry and populate local repository"
                "with the documents."
        ),
        response_model=JobEnqueued)
async def update_document_list(
    request:Request,
    krs:str):
    log.info(f"Updating financial documents for KRS {krs}")
    log.debug("Enqueuing job resposible for downloading documents")
    job_id = str(uuid.uuid4())
    queue = request.app.state.queues["KRSDF"]
    job = queue.enqueue(
        task_scrape_documents,
        job_id,
        krs,
        job_id=job_id)
    return JobEnqueued(
        job_id=job_id,
        job_status_url="",
        message="Job was successfully enqueued"
    )

@router.get(
    "/update-document-list-job-status/{job_id}",
    summary=(
            "Use this endpoint to check status of job responsible for fetching"
            "documents from official KRS repository. Job is responsible for fetching,"
            "and loading the documents into local repository."
    ),
    response_model=JobStatus)
async def update_document_list_job_status(
    request:Request,
    job_id:str):
    log.info(f"Fetching information about job status for job id: {job_id}")
    redis_conn = request.app.state.redis
    try: 
        log.debug("Trying to fetch job info from redis queue")
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception as e:
        log.warning(f"Could not fetch information about job from redis queue")
        raise HTTPException(
            status_code=404,
            detail="Job not found")
    else:
        log.debug("Returning information about job status to client")
        return JobStatus(
            job_id=job.id,
            job_status=job.get_status(),
            job_enqueued_at=job.enqueued_at,
            job_started_at=job.started_at,
            job_ended_at=job.ended_at,
            job_result=job.result,
            job_exc_info=job.exc_info,)

@router.get(
        "/available-documents/{krs}",
        summary=(
                "Use this endpoint to check what documents are currently"
                "available in the local repository."
        ),
        response_model=AvailableKRSDFDocuments)
async def available_documents(
    request:Request,
    krs:str):
    log.info(f"Fetching list of locally available documents for krs {krs}")
    async with request.app.state.psql_async_sessionmaker() as session:
        stmt = (
            select(
                KRSDFDocuments.document_type,
                KRSDFDocuments.document_date_from,
                KRSDFDocuments.document_date_to,
                KRSDFDocuments.hash_id
            )
            .where(KRSDFDocuments.krs_number==krs)
        )
        result = await session.execute(stmt)
        result = result.all()
        log.debug(f"Found {len(result)} documents in local depository")
        document_info = [
            DocumentInfo(
                document_name=r.document_type,
                document_date_from=r.document_date_from,
                document_date_to=r.document_date_to,
                document_hash_id=r.hash_id)
            for r in result]
        return AvailableKRSDFDocuments(
            document_list=document_info)

@router.post(
        "/download-available-documents",
        summary=(
                "Use this endpoint to download selected documents from"
                "the local document repository."
        ),
        response_model=None,
        response_class=StreamingResponse)
async def download_available_documents(
    request:Request,
    data:RequestHashIDs):
    async with request.app.state.psql_async_sessionmaker() as session:
        stmt = (
            select(
                    KRSDFDocuments.krs_number,
                    KRSDFDocuments.document_content_save_name,
                    KRSDFDocuments.document_content,
                    KRSDFDocuments.hash_id
            )
            .where(KRSDFDocuments.hash_id.in_(data.hash_ids))
        )
        result = await session.execute(stmt)
        result = result.all()
    fetched_hash_ids = [r.hash_id for r  in result]
    missing_hash_ids = [hash_id for hash_id in data.hash_ids 
                        if hash_id not in fetched_hash_ids]
    if missing_hash_ids:
        error_message = (
            f"\nCould not fetch all documents from the db"
            f"\nMaybe some of them were not scraped yet?"
            f"\nMissing hash ids: {'\n'+'\n-'.join(missing_hash_ids)}"
        )
        log.error(error_message)
        raise HTTPException(
            status_code=500,
            detail=error_message
        )
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for row in result:
            zip_file.writestr(
                f"{row.krs_number}_{row.document_content_save_name}",
                row.document_content)
    zip_stream.seek(0)
    return StreamingResponse(
        zip_stream,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=documents.zip"})