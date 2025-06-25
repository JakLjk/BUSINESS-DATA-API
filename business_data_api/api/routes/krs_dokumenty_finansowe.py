from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.requests import Request
from typing import Literal, List
from urllib.parse import urlencode
from rq.job import Job
from sqlalchemy import select
import uuid

from business_data_api.utils.dict_response_template import compile_message
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import (KRSDokumentyFinansowe,
                                                                        task_get_document_list,
                                                                        task_get_documents_contents)
from business_data_api.db.models import ScrapedKrsDF, ScrapingStatus, RedisScrapingRegistry

router = APIRouter()

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
    with psqlsession.begin()
        hash_ids_to_scrape = []
        existing_records = (
            psql_session.query(RedisScrapingRegistry)
            .filter(RedisScrapingRegistry.in_(hash_ids))
            .with_for_update()
            .all()
        )
        existing_records = [record.hash_id: record for record in existing_records]
        for hash_id in hash_ids:
            record = existing_records.get(hash_id)
            if record:
                pass
            else:
                # New records not in DB - scrape them (while checking if they were not added
                # during the transaction, since Read Commited is default psql behaviour)

    

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




