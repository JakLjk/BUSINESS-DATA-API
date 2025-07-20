import os
import uuid
from typing import Literal
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from fastapi.requests import Request
from rq.job import Job
from sqlalchemy import select


from business_data_api.utils.logger import setup_logger
from business_data_api.db.models import CompanyInfo
from business_data_api.workers.tasks.scraping_krs_api.scrape_extract import task_scrape_krs_api_extract
from business_data_api.api.models import (
    JobEnqueued,
    JobStatus,
    CompanyInfoResponse,)


load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
log = setup_logger(
    logger_name="route_krs_api",
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url)

router = APIRouter()


@router.get(
    "/health", 
    summary="API KRS route health check")
async def health():
    log.info(f"Returning information about health status")
    return "Health ok.", 200

@router.get(
        "/update-business-information/{krs}",
        summary=(
            "Use this endpoint to update informations about business  "
            "based on current info in official KRS API"
        ),
        response_model=JobEnqueued)
async def update_business_information(
    request: Request,
    krs: str):
    log.info(f"Updating business information for KRS {krs}")
    log.debug("Enqueuing job resposible for updating KRS API information")
    job_id = str(uuid.uuid4())
    queue = request.app.state.queues["KRSAPI"]
    job = queue.enqueue(
        task_scrape_krs_api_extract,
        job_id,
        krs,
        job_id=job_id)
    log.debug(f"Returning information about job enqueued to client")
    return JobEnqueued(
        job_id=job_id,
        job_status_url="",
        message="Job was successfully enqueued")
        
@router.get(
    "update-business-information-job-status/{job_id}",
    summary=(
        "Use this endpoint to check status of job responsible for fetching business"
        "information from official KRS API endpoint. Job is responsible for"
        "fetching, transforming and loading information about business into"
        "local repository."
    ),
    response_model=JobStatus)
async def update_business_information_job_status(
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
    "/download-business-information/{krs}",
    summary=(
        "Use this endpoint to download informations about business,"
        "that are available in the local repository"
    ),
    response_model=CompanyInfoResponse)
async def download_business_information(
    request:Request,
    krs:str):
    log.info(f"Fetching business informations from local repository. krs: {krs}")
    async with request.app.state.psql_async_sessionmaker() as session:
        stmt = (
            select(CompanyInfo)
            .where(
                CompanyInfo.krs_number==krs,
                CompanyInfo.is_current==True
            )
        )
        result = await session.execute(stmt)
        company = result.scalars().first()
    if company is None:
        log.error(f"Could not find information about krs {krs} in local DB")
        raise HTTPException(
            status_code=404,
            detail="Company not found")
    log.debug(f"Returning information about business to client")
    return company