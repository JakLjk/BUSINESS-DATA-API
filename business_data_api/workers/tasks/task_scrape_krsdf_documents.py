import os
from typing import List
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
from rq.job import Job
from rq.exceptions import NoSuchJobError, InvalidJobOperation
from redis import Redis

from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.db import create_sync_sessionmaker
from business_data_api.db.models import ScrapedKrsDF, ScrapingStatus, RedisScrapingRegistry, ScrapingCommissions
from business_data_api.utils.logger import setup_logger
from business_data_api.utils.redis_utils.redis_job_status import job_is_running


load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
psql_sync_url = os.getenv("SYNC_POSTGRE_URL")
redis_url = os.getenv("REDIS_URL")
stale_job_treshold_seconds = os.getenv()

def task_scrape_krsdf_documents(job_id: str, krs: str, hash_ids: List[str]):
    log_scrape_documents = setup_logger(
    logger_name=f"worker_task_get_documents_data",
    logger_id=job_id,
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )
    log_scrape_documents.info("Task - fetching document data")
    log_scrape_documents.debug("Initialising PostgreSQL session")
    session = create_sync_sessionmaker(psql_sync_url)
    log_scrape_documents.debug("Initialising Redis session")
    redis_conn = Redis.from_url(redis_url)

    log_scrape_documents.debug("Initialising KRS Scraper object")
    krsdf = KRSDokumentyFinansowe(krs)
    log_scrape_documents.debug("Initialising download documents function")
    krsdf.download_documents(hash_ids)
    while (hash_id := krsdf.download_documents_next_id()):
        log_scrape_documents.debug(f"Scraping document: {hash_id}")
        log_scrape_documents.debug("Beggining PSQL transaction")
        with session.begin():
            other_instances_of_hash_id = (
                session.query(ScrapingCommissions)
                .filter(
                    ScrapingCommissions.hash_id == hash_id,
                    ScrapingCommissions.job_id != job_id
                )
                .all()
            )
            current_record = (
                session.query(ScrapingCommissions)
                .filter(
                    ScrapingCommissions.hash_id == hash_id,
                    ScrapingCommissions.job_id != job_id
                )
                .first()
            )
            if other_instances_of_hash_id:
                finished_jobs_for_hash_id = any(
                    r.status == ScrapingStatus.FINISHED for r in other_instances_of_hash_id
                )
                pending_jobs_hash_ids = any(
                    r.status == ScrapingStatus.PENDING for r in other_instances_of_hash_id
                )
                failed_jobs_hash_ids = any(
                    r.status == ScrapingStatus.PENDING for r in other_instances_of_hash_id
                )
                if finished_jobs_for_hash_id:
                    # TODO additional check if record is actually in the DB
                    current_record.status=ScrapingStatus.FINISHED
                    current_record.message="Record was already scraped"
                    session.commit()
                elif pending_jobs_hash_ids:
                    pending_jobs_records = [r for r in other_instances_of_hash_id if r.status==ScrapingStatus.PENDING]
                    try:
                        data = krsd.download_documents_scrape_id()
                        scraped_data = ScrapedKrsDF(**data)
                        session.flush()
                    except IntegrityError:
                        session.rollback()
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message=(
                            "\nRecord was already inserted by another worker"
                        )
                        session.commit()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nRe-scraping FAILED record failed"
                            f"\nDocument could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        current_record.status=ScrapingStatus.FAILED
                        current_record.message=error_message
                        session.commit()

                elif failed_jobs_hash_ids:
                    try:
                        data = krsd.download_documents_scrape_id()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nRe-scraping FAILED record failed"
                            f"\nDocument could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        current_record.status = ScrapingStatus.FAILED
                        current_record.message = error_message
                        session.commit()
                    else:
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message="Succesfully re-scraped document"
                        scraped_data = ScrapedKrsDF(**data)
                        session.commit()
                else:
                    try:
                        data = krsd.download_documents_scrape_id()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nDocument could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        current_record.status = ScrapingStatus.FAILED
                        current_record.message = error_message
                        session.commit()
                    else:
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message="Succesfully scraped document"
                        scraped_data = ScrapedKrsDF(**data)
                        session.commit()

                    try:
                        log_scrape_documents.debug(f"Scraping document {hash_id}")
                        data = krsdf.download_documents_scrape_id()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nRe-scraping FAILED record failed"
                            f"\nDocument could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        log_scrape_documents.error(error_message)
                        existing_record.job_id = job_id
                        existing_record.job_status = ScrapingStatus.FAILED
                        existing_record.scraping_error_message = str(e)
                        session.commit()
                        log_scrape_documents.debug(f"Committed failed job update to DB")
                    else:
                        log_scrape_documents.debug(f"Successfully re-scraped document {hash_id}")
                        log_scrape_documents.debug(f"Updating record as FINISHED for document {hash_id}")
                        existing_record.job_id = job_id
                        existing_record.job_status = ScrapingStatus.FINISHED
                        existing_record.scraping_error_message = ""
                        record_scraped_krsdf = ScrapedKrsDF(**data)
                        session.merge(record_scraped_krsdf)
                        session.commit()
                        log_scrape_documents.debug(f"Committed FINISHED job update to DB")
            # New hash_id that was not scraped before
            else:
                try:
                    data = krsd.download_documents_scrape_id()
                except Exception as e:
                    error_message = (
                        f"\nError has occurred during scraping process"
                        f"\nRe-scraping FAILED record failed"
                        f"\nDocument could not be scraped. id: {hash_id}"
                        f"\nMarking document scraping status as FAILED"
                        f"\nError message: {str(e)}"
                    )
                    current_record.status = ScrapingStatus.FAILED
                    current_record.message = error_message
                    session.commit()
                else:
                    current_record.status=ScrapingStatus.FINISHED
                    current_record.message="Succesfully re-scraped document"
                    scraped_data = ScrapedKrsDF(**data)
                    session.commit()