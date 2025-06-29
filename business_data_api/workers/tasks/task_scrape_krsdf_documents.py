import os
from typing import List
from sqlalchemy import text
from dotenv import load_dotenv
from rq.job import Job
from rq.exceptions import NoSuchJobError
from redis import Redis
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.db import psql_syncsession
from business_data_api.db.models import ScrapedKrsDF, ScrapingStatus, RedisScrapingRegistry
from business_data_api.utils.logger import setup_logger



def task_scrape_krsdf_documents(job_id: str, krs: str, hash_ids: List[str]):
    logger = setup_logger(f"krsdf_job_{job_id}")
    logger.debug("Loading env variables")
    load_dotenv()
    redis_url = os.getenv("REDIS_URL")
    logger.info("Initialising PostgreSQL session")
    session = psql_syncsession()
    logger.info("Initialising Redis session")
    redis_conn = Redis.from_url(redis_url)

    # List of all hash ids found by the scraping function
    # It is used to evaluate if all provided hash ids were found
    found_hash_ids = []

    logger.info("Initialising KRS Scraper object")
    krsdf = KRSDokumentyFinansowe(krs)
    logger.info("Initialising download documents function")
    krsdf.download_documents(hash_ids)
    while (hash_id := krsdf.download_documents_next_id()):
        logger.debug(f"Scraping document: {hash_id}")
        logger.debug("Starting PSQL session")
        with session.begin():
            # Lock and wait for the row if it is used by another process
            # TODO - check this logic
            hash_bytes = bytes.fromhex(hash_id)
            hash_id_int = int.from_bytes(hash_bytes[:8], byteorder='big', signed=False)
            if hash_id_int > 0x7FFFFFFFFFFFFFFF:
                hash_id_int -= 0x10000000000000000
            logger.debug("Locking row in table")
            session.execute(
                text("SELECT pg_advisory_xact_lock(:hash_id);"),
                {"hash_id":hash_id_int}
            )
            logger.debug("Searching for exisiting record in registry")
            existing_record = (
                session.query(RedisScrapingRegistry)
                .filter(RedisScrapingRegistry.hash_id == hash_id)
                .first()
            )
            if existing_record:
                if existing_record.job_status == ScrapingStatus.FAILED:
                    logger.debug("Found existing record with status FAILED - retrying scraping")
                    previous_job_id = existing_record.job_id 
                    try:
                        job = Job.fetch(previous_job_id, connection=redis_conn)
                        job.cancel()
                        logger.warning("Cancelled stale job for FAILED scraping job")
                    except NoSuchJobError:
                        pass
                    try:
                        logger.debug(f"Scraping document {hash_id}")
                        data = krsdf.download_documents_scrape_id()
                    except Exception as e:
                        logger.error(f"Error has occurred during scraping process: {str(e)}")
                        existing_record.job_id = job_id
                        existing_record.job_status = ScrapingStatus.FAILED
                        existing_record.scraping_error_message = str(e)
                        session.commit()
                        logger.debug(f"Committed changes to DB")
                    else:
                        logger.debug(f"Successfully scraped document {hash_id}")
                        existing_record.job_id = job_id
                        existing_record.job_status = ScrapingStatus.FINISHED
                        existing_record.scraping_error_message = ""
                        record_scraped_krsdf = ScrapedKrsDF(**data)
                        session.merge(record_scraped_krsdf)
                        session.commit()
                        logger.debug(f"Committed changes to DB")
                elif existing_record.job_status == ScrapingStatus.PENDING:
                    logger.debug("Found existing record with status PENDING - checking if job is stale")
                    try:
                        job = Job.fetch(previous_job_id, connection=redis_conn)
                    except NoSuchJobError:
                        logger.warning("Job with status PENDING has not been found")
                        try:
                            logger.debug(f"Scraping document {hash_id}")
                            data = krsdf.download_documents_scrape_id()
                        except Exception as e:
                            logger.error(f"Error has occurred during scraping process: {str(e)}")
                            existing_record.job_id = job_id
                            existing_record.job_status = ScrapingStatus.FAILED
                            existing_record.scraping_error_message = str(e)
                            session.commit()
                            logger.debug(f"Committed changes to DB")
                        else:
                            logger.debug(f"Successfully scraped document {hash_id}")
                            existing_record.job_id = job_id
                            existing_record.job_status = ScrapingStatus.FINISHED
                            existing_record.scraping_error_message = ""
                            record_scraped_krsdf = ScrapedKrsDF(**data)
                            session.merge(record_scraped_krsdf)
                            session.commit()
                            logger.debug(f"Committed changes to DB")
                elif existing_record.job_status == ScrapingStatus.FINISHED:
                    logger.debug(f"Document has already been scraped {hash_id}")
                    krsdf.download_documents_skip_id()
                else:
                    raise ValueError("Improper value of ScrapingStauts in DB table")
            else:
                logger.debug(f"No existing record was found for document {hash_id}")
                try:
                    logger.debug(f"Scraping document {hash_id}")
                    data = krsdf.download_documents_scrape_id()
                except Exception as e:
                    logger.error(f"Error has occurred during scraping process: {str(e)}")
                    record_redis_registry = RedisScrapingRegistry(
                        hash_id = hash_id,
                        job_id = job_id,
                        job_status = ScrapingStatus.FAILED,
                        scraping_error_message = str(e))
                    session.add(record_redis_registry)
                    session.commit()
                    logger.debug(f"Committed changes to DB")
                else:
                    logger.debug(f"Successfully scraped document {hash_id}")
                    record_scraped_krsdf = ScrapedKrsDF(**data)
                    record_redis_registry = RedisScrapingRegistry(
                        hash_id = hash_id,
                        job_id = job_id,
                        job_status = ScrapingStatus.FINISHED,
                        scraping_error_message = "")
                    session.add(record_scraped_krsdf)
                    session.add(record_redis_registry)
                    session.commit()
                    logger.debug(f"Committed changes to DB")
