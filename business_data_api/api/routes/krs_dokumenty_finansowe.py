import os
import uuid
import io
import zipfile

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from typing import Literal, List, Union
from urllib.parse import urlencode
from rq.job import Job
from rq.exceptions import NoSuchJobError, InvalidJobOperation
from sqlalchemy import select
from dotenv import load_dotenv
from datetime import datetime



from business_data_api.workers.tasks.task_scrape_krsdf_document_list import task_get_document_list
from business_data_api.workers.tasks.task_scrape_krsdf_documents import task_scrape_krsdf_documents
from business_data_api.db.models import KRSDFDocuments, DocumentScrapingStatus, ScrapingStatus
from business_data_api.utils.logger import setup_logger
from business_data_api.workers.tasks.status import JobTaskStatus
from business_data_api.db.models import ScrapingStatus
from business_data_api.utils.response_templates.default_response import APIResponse, ResponseStatus
from business_data_api.utils.response_templates.default_response import (
                                                                            DocumentNamesData,
                                                                            DocumentNamesScrapingStatusData,
                                                                            DocumentScrapingStatusData,
                                                                            JobEnqueuedData,
                                                                            HashIdsRequest
                                                                        )

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
    summary="Add job that will fetch names of available documents for KRS number",
    response_model=APIResponse[JobEnqueuedData])
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
        status=ResponseStatus.SUCCESS,
        title="Scraping task was added to queue",
        message=f"go to {request.base_url}krs-df/get-document-names-result/{job_id} to see the results",
        data=JobEnqueuedData(
            job_id=job_id, 
            job_enqueued_at=job.enqueued_at,
            job_task="get_document_names",
            job_variables={"krs":krs})
    )
@router.get(
    "/get-document-names-result/{job_id}",
    name="get_document_names_result",
    summary= (
        "\nGet result of job fetching document names"
        "\nOr get job status if the job has not yet finished"
    ),
    response_model=APIResponse[Union[DocumentNamesData,DocumentNamesScrapingStatusData]])
async def get_document_names_result(
    request: Request,
    job_id: str
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug(
        f"\nStarting process of fetching document names job information "
        f"\nJob id:{job_id}")
    redis_conn = request.app.state.redis
    try:
        log_api_krsdf.debug(f"Fetching information about job id: {job_id}")
        job = Job.fetch(job_id, connection=redis_conn)
    except NoSuchJobError as e:
        error_message = (
            f"\nUnable to find job  that is supposed to"
            f"\nfetch document names."
            f"\nJob id: {job_id}"
            f"\nError: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=404,
            detail=error_message
        )
    except InvalidJobOperation as e:
        error_message = (
            f"\nUnable to find job that is supposed to"
            f"\nfetch document names."
            f"\nJob id: {job_id}"
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
            f"\nof fetching job status responsible for"
            f"\nfetching document names"
            f"\nJob id: {job_id}"
            f"\nError: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=500,
            detail=error_message
        )
    if job.is_finished:
        log_api_krsdf.debug(
            f"\nJob responsible for fetching document names"
            f"\nhas finished "
            f"\nJob id: {job_id}"
            )
        log_api_krsdf.debug("Checking task completion status")
        job_task_status = job.result["status"]
        if job_task_status == JobTaskStatus.SUCCESS:
            log_api_krsdf.debug(
                f"\nTask was completed successfully"
                f"\nReturning document names"
            )
            return APIResponse(
                status=ResponseStatus.SUCCESS,
                title="Document names retrieved",
                message="Requested document names were retrieved from host",
                data=DocumentNamesData(documents=job.result["document_list"])
        )
        elif job_task_status == JobTaskStatus.FAILED:
            log_api_krsapi.error(
                f"\nJob has finished running, but scraping task has failed"
            )
            return APIResponse(
                status=FAILED,
                title="Job has finished running but task has failed",
                message=(
                    "\nJob responsible for fetching document names list"
                    "\nhas finished, but the task responsible for feching documents has failed"
                ),
                data=JobStatusData(
                    job_id=job_id,
                    message="Job has finishe - task has failed",
                    job_task_status=status,
                    job_runtime_status=job.get_status(),
                    job_execution_info=None,
                    job_enqueued_at=None,
                    job_started_at=None
                )
            )
    elif job.is_failed:
        error_message = (
            f"\nJob scraping document names list has failed." 
            f"\nJob id: {job_id}"
            f"\nJob execution info: {job.exc_info}"
            f"\nJob id:{job_id}" 
            f"\ne rror:{job.exc_info}"
            )
        log_api_krsdf.error(error_message)
        return APIResponse(
            status=ResponseStatus.FAILED,
            title="Scraping job failed",
            message=error_message,
            data=DocumentNamesScrapingStatusData(
                job_id=job_id,
                job_task_status=None,
                job_runtime_status=job.get_status(),
                job_execution_info=job.exc_info(),
                job_enqueued_at=None,
                job_started_at=None
            )
        )
    elif job.is_queued or job.is_started:
        log_api_krsdf.debug(f"Job is currently in progress. id:{job_id}")
        return APIResponse(
            status=ResponseStatus.PENDING,
            title="Job is in progress",
            message="Job is being server by worker",
            data=DocumentNamesScrapingStatusData(
                job_id=job_id,
                job_task_status=None,
                job_runtime_status=job.get_status(),
                job_execution_info=None,
                job_enqueued_at=job.enqueued_at,
                job_started_at=job.started_at
            )
        )

@router.post(
    "/scrape-documents/{krs}",
    summary=(
        "Scrape documents from the official KRS site"
        "by providing company KRS number and hash ids of documents to scrape"
        ),
    response_model=APIResponse[JobEnqueuedData])
async def scrape_documents(
    request: Request,
    krs:str,
    data: HashIdsRequest
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug("Loading payload variables")
    hash_ids = data.hash_ids
    log_api_krsdf.debug(f"Generating job id")
    job_id = str(uuid.uuid4())
    log_api_krsdf.debug(f"Opening PSQL session")
    async with request.app.state.psql_async_sessionmaker() as session:
        log_api_krsdf.debug("Adding information about job status to DB")
        for hash_id in hash_ids:
            status = DocumentScrapingStatus(
                job_id=job_id,
                hash_id=hash_id,
                scraping_status=ScrapingStatus.PENDING,
                message="Job responsible for fetching documents was added to queue"
            )
            session.add(status)
        await session.commit()
        log_api_krsdf.debug("Added information about scraping status to DB")
    log_api_krsdf.debug(f"Enqueuing scraping job")
    queue = request.app.state.queues["KRSDF"]
    job = queue.enqueue(task_scrape_krsdf_documents, job_id, krs, hash_ids, job_id=job_id)
    log_api_krsdf.debug(f"Sending response to client")
    return APIResponse(
        status=ResponseStatus.SUCCESS,
        title="Scraping job for added to queue",
        message=f"Scraping of {len(hash_ids)} documents was added to the queue",
        data=JobEnqueuedData(
            job_id=job_id,
            job_enqueued_at=job.enqueued_at,
            job_task="scrape_documents",
            job_variables={
                "krs":krs,
                "hash_ids":hash_ids
            }
        )
    )
        
@router.post(
    "/documents-scraping-status/{job_id}",
    summary=(
        "Check status of documents."
        "Documents can be already scraped, or in queue, or not being"
        "processed at all."
        ),
    response_model=APIResponse[DocumentScrapingStatusData]
    )
async def documents_scraping_status(
    request: Request,
    job_id:str
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug("Fetching Redis connector")
    redis_conn = request.app.state.redis
    log_api_krsdf.debug(f"Checking if scraping job is still live")
    try:
        log_api_krsdf.debug(f"Fetching information about job id: {job_id}")
        job = Job.fetch(job_id, connection=redis_conn)
    except NoSuchJobError or InvalidJobOperation as e:
        log_api_krsdf.debug("Could not find job with specified id")
        log_api_krsdf.debug("Searching for job results in repository")
        async with request.app.state.psql_async_sessionmaker() as session:
            job_records = await session.execute(
                    select(DocumentScrapingStatus)
                    .where(DocumentScrapingStatus.job_id==job_id)
                )
            job_records = job_records.scalars().all()
            if not job_records:
                error_message = (
                    f"\nJob could not be found"
                    f"\nand historic logs do not contain"
                    f"\ninformation about this job results"
                    f"\nJob id: {job_id}"
                )
                log_api_krsdf.error(error_message)
                raise HTTPException(
                    status_code=404,
                    detail=error_message
                )
            statuses = {r.hash_id:r.scraping_status for r in job_records}
        message = (
            "\nJob could not be found, but records"
            "\nAttached to this job id were found"
        )
        log_api_krsdf.debug(message)
        return APIResponse(
            status=ResponseStatus.SUCCESS,
            title="Successfully fetched document scraping status",
            message=message,
            data=DocumentScrapingStatusData(
                job_id=job_id,
                job_runtime_status=None,
                job_execution_info=None,
                job_enqueued_at=None,
                job_started_at=None,
                hash_id_statuses=statuses
            )
        )
    except Exception as e:
        error_message = (
            f"\nUnknown error has occurred during process"
            f"\nof fetching job responsible for"
            f"\nscraping documents"
            f"\nJob id: {job_id}"
            f"\nError: {str(e)}"
        )
        log_api_krsdf.error(error_message)
        raise HTTPException(
            status_code=500,
            detail=error_message
        )
    else:
        if job.is_finished:
            log_api_krsdf.debug(f"Job has finished running")
            log_api_krsdf.debug(f"Searching for job results in repository")
            async with request.app.state.psql_async_sessionmaker() as session:
                job_records = await session.execute(
                    select(DocumentScrapingStatus)
                    .where(DocumentScrapingStatus.job_id==job_id)
                )
                job_records = job_records.scalars().all()
                statuses = {r.hash_id:r.scraping_status for r in job_records}
            message = (f"Returning information about scraping statuses")
            log_api_krsdf.debug(message)
            return APIResponse(
                status=ResponseStatus.SUCCESS,
                title="Successfully fetched document scraping status",
                message=message,
                data=DocumentScrapingStatusData(
                    job_id=job_id,
                    job_runtime_status=job.get_status(),
                    job_execution_info=None,
                    job_started_at=job.started_at,
                    job_enqueued_at=job.enqueued_at,
                    hash_id_statuses=statuses
                )
            )
        elif job.is_failed:
            error_message = (f"Job responsible for scraping document data has failed")
            log_api_krsdf.error(error_message)
            return APIResponse(
                status=ResponseStatus.FAILED,
                title="Job has failed",
                message=error_message,
                data=DocumentScrapingStatusData(
                    job_id=job_id,
                    job_runtime_status=job.get_status(),
                    job_execution_info=job.exc_info,
                    job_started_at=job.started_at,
                    job_enqueued_at=job.enqueued_at,
                    hash_id_statuses={}
                )
            )
        elif job.is_queued or job.is_started:
            message = (f"Job is still running")
            log_api_krsdf.debug(message)
            log_api_krsdf.debug("Fetching information about current document scraping statuses")
            async with request.app.state.psql_async_sessionmaker() as session:
                job_records = await session.execute(
                    select(DocumentScrapingStatus)
                    .where(DocumentScrapingStatus.job_id==job_id)
                )
                job_records = job_records.scalars().all()
                statuses = {r.hash_id:r.scraping_status for r in job_records}
            return APIResponse(
                status=ResponseStatus.PENDING,
                title="Job is still being processed",
                message=message,
                data=DocumentScrapingStatusData(
                    job_id=job_id,
                    job_runtime_status=job.get_status(),
                    job_execution_info=None,
                    job_started_at=job.started_at,
                    job_enqueued_at=job.enqueued_at,
                    hash_id_statuses=statuses
                )
            )
        else:
            error_message = (
                f"\nCould not process the job status"
                f"\nJob id: {job_id}"
            )
            log_api_krsdf.error(error_message)
            raise HTTPException(
                status_code=500,
                detail=error_message
            )

@router.post(
    "/download-documents",
    summary=(
        "Download documents by providing their hash_ids",
        "Documents will be fetched from local PostgreSQL DB (If exists)"
        "And packed into ZIP file and then send as Streaming Response"
        "IF documements are not available in db, they won't be downloaded"
    ),
    response_model=None,
    response_class=StreamingResponse
    )
async def download_documents(
    request: Request,
    data: HashIdsRequest
    ):
    log_api_krsdf.info(f"Requested site: {request.url}")
    log_api_krsdf.debug(f"Loading payload variables")
    hash_ids = data.hash_ids
    log_api_krsdf.debug(f"Opening PostgreSQL DB session")
    async with request.app.state.psql_async_sessionmaker() as session:
        log_api_krsdf.debug("Fetching information about records that are to be downloaded")
        query = select(KRSDFDocuments).where(KRSDFDocuments.hash_id.in_(hash_ids))
        results = await session.execute(query)
        rows = results.scalars().all()
    log_api_krsdf.debug(f"Looking for missing hash ids")
    fetched_hash_ids = [r.hash_id for r in rows]
    missing_hash_ids = [hash_id for hash_id in hash_ids if hash_id not in fetched_hash_ids]
    if len(missing_hash_ids) > 0:
        error_message = (
            f"\nCould not fetch all documents from the db"
            f"\nMaybe some of them were not scraped yet?"
            f"\nMissing hash ids: {'\n'+'\n-'.join(missing_hash_ids)}"
        )
        log_api_krsapi.error(error_message)
        raise HTTPException(
            status_code=500,
            detail=error_message
        )
        
    log_api_krsdf.debug("Creating zip stream and packing data into ZIP file")
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for row in rows:
            zip_file.writestr(row.document_content_save_name, row.document_content)
    zip_stream.seek(0)
    log_api_krsdf.debug(f"Returning zipped documents to user")
    return StreamingResponse(
        zip_stream,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=documents.zip"}
    )




