import os
from typing import List
from sqlalchemy import text
from dotenv import load_dotenv
from rq.job import Job
from rq.exceptions import NoSuchJobError, InvalidJobOperation
from redis import Redis

from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.db import psql_sync_session
from business_data_api.db.models import ScrapedKrsDF, ScrapingStatus, RedisScrapingRegistry
from business_data_api.utils.logger import setup_logger


load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
redis_url = os.getenv("REDIS_URL")

def task_scrape_krsdf_documents(job_id: str, krs: str, hash_ids: List[str]):
    log_scrape_documents = setup_logger(
    logger_name=f"worker_task_get_documents_data",
    logger_id=job_id,
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )
    log_scrape_documents.info("Task - fetching document data")
    log_scrape_documents.debug("Initialising PostgreSQL session")
    session = psql_syncsession()
    log_scrape_documents.debug("Initialising Redis session")
    redis_conn = Redis.from_url(redis_url)

    # List of all hash ids found by the scraping function
    # It is used to evaluate if all provided hash ids were found
    found_hash_ids = []

    log_scrape_documents.debug("Initialising KRS Scraper object")
    krsdf = KRSDokumentyFinansowe(krs)
    log_scrape_documents.debug("Initialising download documents function")
    krsdf.download_documents(hash_ids)
    while (hash_id := krsdf.download_documents_next_id()):
        log_scrape_documents.debug(f"Scraping document: {hash_id}")
        log_scrape_documents.debug("Beggining PSQL transaction")
        with session.begin():
            # Lock and wait for the row if it is used by another process
            log_scrape_documents.debug("Generating hash index for locking row in DB")
            hash_bytes = bytes.fromhex(hash_id)
            hash_id_int = int.from_bytes(hash_bytes[:8], byteorder='big', signed=False)
            if hash_id_int > 0x7FFFFFFFFFFFFFFF:
                hash_id_int -= 0x10000000000000000
            log_scrape_documents.debug("Locking row in table")
            session.execute(
                text("SELECT pg_advisory_xact_lock(:hash_id);"),
                {"hash_id":hash_id_int}
            )
            logger.log_scrape_documents("Searching for exisiting record in registry")
            existing_record = (
                session.query(RedisScrapingRegistry)
                .filter(RedisScrapingRegistry.hash_id == hash_id)
                .first()
            )
            if existing_record:
                log_scrape_documents.info("Found existing record in the DB")
                if existing_record.job_status == ScrapingStatus.FAILED:
                    log_scrape_documents.debug("Found existing record with status FAILED - retrying scraping")
                    previous_job_id = existing_record.job_id 
                    try:
                        log_scrape_documents.debug("Trying to close stale job with FAILED scraping status")
                        job = Job.fetch(previous_job_id, connection=redis_conn)
                        job.cancel()
                        logger.warning("Cancelled stale job for FAILED scraping job")
                    except NoSuchJobError:
                        log_scrape_documents.debug("No stale FAILED job found")
                        pass
                    except InvalidJobOperation:
                        log_scrape_documents.debug("No stale FAILED job found")
                        pass
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
                elif existing_record.job_status == ScrapingStatus.PENDING:
                    log_scrape_documents.debug("Found existing record with status PENDING - checking if job is stale")
                    try:
                        job = Job.fetch(previous_job_id, connection=redis_conn)
                    except NoSuchJobError:
                        log_scrape_documents.warning("Job with status PENDING has not been found")
                        try:
                            log_scrape_documents.debug(f"Scraping document {hash_id}")
                            data = krsdf.download_documents_scrape_id()
                        except Exception as e:
                            error_message = (
                                f"\nError has occurred during scraping process"
                                f"\nDocument could not be scraped. id: {hash_id}"
                                f"\nMarking document scraping status as FAILED"
                                f"\nError message: {str(e)}"
                            )
                            log_scrape_documents.error(error_message)
                            existing_record.job_id = job_id
                            existing_record.job_status = ScrapingStatus.FAILED
                            existing_record.scraping_error_message = str(e)
                            session.commit()
                            log_scrape_documents.debug(f"Committed FAILED job update to DB")
                        else:
                            log_scrape_documents.debug(f"Successfully scraped document {hash_id}")
                            existing_record.job_id = job_id
                            existing_record.job_status = ScrapingStatus.FINISHED
                            existing_record.scraping_error_message = ""
                            record_scraped_krsdf = ScrapedKrsDF(**data)
                            session.merge(record_scraped_krsdf)
                            session.commit()
                            log_scrape_documents.debug(f"Committed FINISHED job update to DB")
                elif existing_record.job_status == ScrapingStatus.FINISHED:
                    log_scrape_documents.debug(
                        f"\nDocument already has status FINISHED in DB"
                        f"\nDocument has already been scraped {hash_id}"
                        f"\nSkipping this document in scraping process"
                        )
                    krsdf.download_documents_skip_id()
                else:
                    error_message = (
                        "Invalid value of ScrapingStatus in DB"
                        "Most probably record was not saved correctly"
                    )
                    log_scrape_documents.error(error_message)
                    raise ValueError(error_message)
            else:
                log_scrape_documents.info(f"No existing record was found for document {hash_id}")
                try:
                    log_scrape_documents.debug(f"Scraping document {hash_id}")
                    data = krsdf.download_documents_scrape_id()
                except Exception as e:
                    error_message = (
                        f"\nError has occurred during scraping process"
                        f"\nDocument could not be scraped. id: {hash_id}"
                        f"\nMarking document scraping status as FAILED"
                        f"\nError message: {str(e)}"
                    )
                    log_scrape_documents.error(error_message)
                    record_redis_registry = RedisScrapingRegistry(
                        hash_id = hash_id,
                        job_id = job_id,
                        job_status = ScrapingStatus.FAILED,
                        scraping_error_message = str(e))
                    session.add(record_redis_registry)
                    session.commit()
                    log_scrape_documents.debug(f"Committed FAILED job info to DB")
                else:
                    log_scrape_documents.debug(f"Successfully scraped document {hash_id}")
                    record_scraped_krsdf = ScrapedKrsDF(**data)
                    record_redis_registry = RedisScrapingRegistry(
                        hash_id = hash_id,
                        job_id = job_id,
                        job_status = ScrapingStatus.FINISHED,
                        scraping_error_message = "")
                    session.add(record_scraped_krsdf)
                    session.add(record_redis_registry)
                    session.commit()
                    log_scrape_documents.debug(f"Committed FINISHED job info to DB")
