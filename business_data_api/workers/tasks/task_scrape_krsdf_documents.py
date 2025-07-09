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
    log_scrape_documents.info("Initialising task for scraping document data from krsdf")
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
            log_scrape_documents.debug(f"Fetching information about existing isntances of hash id in DB")
            other_instances_of_hash_id = (
                session.query(ScrapingCommissions)
                .filter(
                    ScrapingCommissions.hash_id == hash_id,
                    ScrapingCommissions.job_id != job_id
                )
                .all()
            )
            log_scrape_documents.debug(f"Fetching information about current hash id record from DB")
            current_record = (
                session.query(ScrapingCommissions)
                .filter(
                    ScrapingCommissions.hash_id == hash_id,
                    ScrapingCommissions.job_id != job_id
                )
                .first()
            )
            if other_instances_of_hash_id:
                log_scrape_documents.debug(f"Found existing instances of hash id in DB")
                log_scrape_documents(f"Checking for jobs with FINISHED status")
                finished_jobs_for_hash_id = any(
                    r.status == ScrapingStatus.FINISHED for r in other_instances_of_hash_id
                )
                log_scrape_documents.debug(f"Checking for jobs with PENDING status")
                pending_jobs_hash_ids = any(
                    r.status == ScrapingStatus.PENDING for r in other_instances_of_hash_id
                )
                log_scrape_documents.debug(f"Checking for jobs with FAILED status")
                failed_jobs_hash_ids = any(
                    r.status == ScrapingStatus.PENDING for r in other_instances_of_hash_id
                )
                if finished_jobs_for_hash_id:
                    log_scrape_documents.debug(f"Found another record with FINISHED status")
                    # TODO additional check if record is actually in the DB
                    log_scrape_documents.debug("Marking current scraping job as finished")
                    current_record.status=ScrapingStatus.FINISHED
                    current_record.message="Record was already scraped"
                    session.commit()
                elif pending_jobs_hash_ids:
                    log_scrape_documents.debug(f"Found another record with PENDING status")
                    try:
                        log_scrape_documents.debug(f"Initialising document scraping process")
                        data = krsd.download_documents_scrape_id()
                        log_scrape_documents.debug("Trying to insert scraped data into DB")
                        scraped_data = ScrapedKrsDF(**data)
                        session.flush()
                    except IntegrityError:
                        message = (
                            "\nIntegrity Error after trying to insert scraped record into DB"
                            "\nMost propably another with PENDING status inserted the row first"
                        )
                        log_scrape_documents.warning(message)
                        session.rollback()
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message=(message)
                        session.commit()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nFailed to scrape document that was already PENDING in another process"
                            f"\nDocument that could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        log_scrape_documents.error(error_message)
                        current_record.status=ScrapingStatus.FAILED
                        current_record.message=error_message
                        session.commit()
                    else:
                        message = (
                            "\nRecord was succesfully inserted, although another job"
                            "\nWas scraping it (Another job had PENDING status)"
                        )
                        log_scrape_documents.debug(message)
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message=message
                elif failed_jobs_hash_ids:
                    log_scrape_documents.debug(f"Found another record with FAILED status")
                    try:
                        log_scrape_documents.debug(f"Initialising document scraping process")
                        data = krsd.download_documents_scrape_id()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nRe-scraping FAILED record was not successfull"
                            f"\nDocument that could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        log_scrape_documents.error(error_message)
                        current_record.status = ScrapingStatus.FAILED
                        current_record.message = error_message
                        session.commit()
                    else:
                        log_scrape_documents.debug(f"Successfully re-scraped FAILED document")
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message="Succesfully re-scraped document"
                        scraped_data = ScrapedKrsDF(**data)
                        session.commit()
                else:
                    log_scrape_documents.debug("No other instances of document have been found")
                    try:
                        log_scrape_documents.debug(f"Initialising document scraping process")
                        data = krsd.download_documents_scrape_id()
                    except Exception as e:
                        error_message = (
                            f"\nError has occurred during scraping process"
                            f"\nDocument could not be scraped. id: {hash_id}"
                            f"\nMarking document scraping status as FAILED"
                            f"\nError message: {str(e)}"
                        )
                        log_scrape_documents.debug(error_message)
                        current_record.status = ScrapingStatus.FAILED
                        current_record.message = error_message
                        session.commit()
                    else:
                        log_scrape_documents(f"Successfully scraped document")
                        current_record.status=ScrapingStatus.FINISHED
                        current_record.message="Succesfully scraped document"
                        scraped_data = ScrapedKrsDF(**data)
                        session.commit()
