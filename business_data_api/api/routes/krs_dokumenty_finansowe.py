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
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import (KRSDokumentyFinansowe,
                                                                        task_get_document_list,
                                                                        task_get_documents_contents)
from business_data_api.db.models import ScrapedKrsDF, ScrapingStatus, RedisScrapingRegistry

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

@router.get("/get-documents", summary="Get specified documents for KRS")
async def get_documents(
    request: Request,
    krs: str = Query(..., 
                    min_length=10,
                    max_length=10),
    hash_ids: List[str] = Query(...)
):
    hash_ids = hash_ids
    
    psqlsession = request.app.state.psqlsession()
    redis_conn = request.app.state.redis
    async with psqlsession.begin():
        for hash_id in hash_ids:
            await psqlsession.execute(
                text("SELECT pg_advisory_xact_lock(:hash_id);"),
                {"hash_id":hash_id}
            )
        new_hash_ids_to_scrape = []
        previous_hash_ids_to_scrape_again= []
        existing_records = (
            psql_asession.query(RedisScrapingRegistry)
            .filter(RedisScrapingRegistry.in_(hash_ids))
            .with_for_update()
            .all()
        )
        existing_records = {record.hash_id: record for record in existing_records}
        for hash_id in hash_ids:
            record = existing_records.get(hash_id)
            if record:
                if record.job_status == ScrapingStatus.FAILED:
                    # If the job has failed
                    # Try to fetch the job to see if it still running (migh be bugged or frozen)
                    job_id = record.job_id
                    try:
                        job = Job.fetch(job_id, connection=redis_conn)
                        job.cancel()
                    except NoSuchJobError:
                        pass
                    # Add the record to dictionary of hashes that have to be scraped again
                    previous_hash_ids_to_scrape_again.append(record.hash_id)
                elif record.job_status == ScrapingStatus.PENDING:
                    # Check if job is still alive, if not it means it is stale
                    job_id = record.job_id
                    try:
                        job = Job.fetch(job_id, connection=redis_conn)
                        if job.started_at:
                            max_worker_work_time_duration = os.getenv("MAX_WORKER_JOB_DURATION_SECONDS")
                            time_elapsed = (datetime.now() - job.started_at).total_seconds()
                            # If the worker work time exceeds the time provided as env variable, it should be killed 
                            # And rerun
                            if time_elapsed > max_worker_work_time_duration:
                                job.cancel()
                                previous_hash_ids_to_scrape_again.append(record.hash_id)
                    except NoSuchJobError:
                        # Job is stale
                        # Add the record to dictionary of hashes that have to be scraped again
                        previous_hash_ids_to_scrape_again.append(record.hash_id)
                elif record.job_status == ScrapingStatus.FINISHED:
                    # To nothing - record is already scraped
                    pass
                else:
                    # Unknown status - error
                    raise Exception()
            else:
                new_hash_ids_to_scrape.append(record.hash_id)

        max_worker_task_batch_size = os.getenv("SCRAPE_BATCH_SIZE")
        all_hash_ids_to_scrape = new_hash_ids_to_scrape.extend(previous_hash_ids_to_scrape_again)
        job_batches = [hash_ids[i:i+max_worker_task_batch_size] for i in range(0, len(all_hash_ids_to_scrape), max_worker_task_batch_size)]
        for job_batch in job_batches:
            update_hash_ids = []
            insert_hash_ids = []

            

    

    queue = request.app.state.queues("KRSDF")
    job = queue.enqueue(task_get_documents_contents, krs, hash_ids)
    job_id = job.id

    # KRS and HASH IDs provided
    # Script checks if hash id in db
    # If hash id is in DB with status 'pending', and the used job id does not exist in queue,
    # then record will be added to queue once more - after reinserting clean row indo db with pending status
    # Returned value is link to route which will show status of all hash ids in the database
    # The route showing status will also check if the job that was requested still exists for the pending hashes
    # If not, it will update the status of record as Failed
    # Task script will have to check if the record does not have FAILED status before inserting
    # to mitigate the risk of fastapi setting task as Failed in the moment when the worker inserts task


@router.get("/get-documents-status")
async def get_documents_status():
    # Additional check if status is pending and no job - if so mark as failed
    pass




