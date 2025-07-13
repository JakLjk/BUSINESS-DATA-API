import os
import logging
from typing import List
from sqlalchemy import text, select, exists
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
from rq.job import Job
from rq.exceptions import NoSuchJobError, InvalidJobOperation
from redis import Redis

from business_data_api.utils.logger import setup_logger
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.db import create_sync_sessionmaker
from business_data_api.db.models import KRSDFDocuments, DocumentScrapingStatus, ScrapingStatus

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
psql_sync_url = os.getenv("SYNC_POSTGRE_URL")
redis_url = os.getenv("REDIS_URL")
stale_job_treshold_seconds = os.getenv("STALE_JOB_TRESHOLD_SECONDS")
sessionmaker = create_sync_sessionmaker(psql_sync_url)
redis_conn = Redis.from_url(redis_url)
def task_scrape_krsdf_documents(job_id: str, krs: str, hash_ids: List[str]):
    log_scrape_documents = setup_logger(
    logger_name=f"worker_task_get_documents_data",
    logger_id=job_id,
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )
    log_scrape_documents.info("Initialising task for scraping document data from krsdf")
    log_scrape_documents.debug("Checking if all documents are avaiable locally") 
    document_download_status = {}
    with sessionmaker() as session:
        available_documents = (
            session.query(KRSDFDocuments)
            .filter(KRSDFDocuments.hash_id.in_(hash_ids))
            .all()
        )
    available_hash_ids = [r.hash_id for r in available_documents]
    missing_hash_ids = [hash_id for hash_id in hash_ids if hash_id not in available_hash_ids]
    log_scrape_documents.debug(
        f"\nThere are {len(available_hash_ids)} documents avaialble locally"
        f"\nAnd {len(missing_hash_ids)} documents have to be scraped"
        )
    for hash_id in available_hash_ids:
        set_document_download_status(
            job_id=job_id, 
            hash_id=hash_id,
            message="Document already available locally",
            scraping_status=ScrapingStatus.SUCCESS)
    if missing_hash_ids:
        log_scrape_documents.debug("Initialising KRS Scraper object")
        krsdf = KRSDokumentyFinansowe(krs)
        log_scrape_documents.debug("Initialising download documents function")
        krsdf.download_documents(missing_hash_ids)
        # Track download status of each element
        while (hash_id := krsdf.download_documents_next_id_value()):
            log_scrape_documents.debug(f"Starting process for: {hash_id}")
            log_scrape_documents.debug("Beggining PSQL transaction")
            with sessionmaker() as session:
                log_scrape_documents.debug("Checking if document is already in local repository")
                stmt = select(exists().where(KRSDFDocuments.hash_id == hash_id))
                record_exists = session.scalar(stmt)
            if record_exists:
                log_scrape_documents.debug("Document already in local repository")
                set_document_download_status(
                    job_id=job_id, 
                    hash_id=hash_id,
                    message="Document already available locally",
                    scraping_status=ScrapingStatus.SUCCESS)
                document_download_status[hash_id] = ScrapingStatus.SUCCESS
                log_scrape_documents.debug("Skipping to next document id")
                krsdf.download_documents_skip_id()
            else:
                log_scrape_documents.debug("Document not in local repository")
                log_scrape_documents.debug("Initialising scraping process")
                try:
                    log_scrape_documents.debug(f"Scraping document {hash_id}")
                    document = krsdf.download_documents_scrape_id()
                    document = KRSDFDocuments(**document)
                    insert_document_to_db(document=document)
                    set_document_download_status(
                        job_id=job_id, 
                        hash_id=hash_id,
                        message="Document has been scraped",
                        scraping_status=ScrapingStatus.SUCCESS)
                    document_download_status[hash_id] = ScrapingStatus.SUCCESS
                    log_scrape_documents.debug(f"Document has been successfully scraped")
                except Exception as e:
                    error_message = (
                        f"\nError has occurred during scraping process"
                        f"\nDocument that could not be scraped. id: {hash_id}"
                        f"\nMarking document scraping status as FAILED"
                        f"\nError message: {str(e)}"
                    )
                    log_scrape_documents.error(error_message)
                    set_document_download_status(
                        job_id=job_id, 
                        hash_id=hash_id,
                        message=error_message,
                        scraping_status=ScrapingStatus.FAILED)
                    document_download_status[hash_id] = ScrapingStatus.FAILED
        if len(document_download_status) != len(missing_hash_ids):
            failed_to_scrape_hash_ids = [hash_id for hash_id in missing_hash_ids if hash_id not in document_download_status.keys()]
            error_message = (
                f"\nNot all document statuses were generated"
                f"\nScraped documents: {len(document_download_status)} Enqueued hash ids {len(missing_hash_ids)}"
                f"\nHash ids that could not be scraped: {str(failed_to_scrape_hash_ids)}"
            )
            log_scrape_documents.error(error_message)
            set_task_as_failed(job_id=job_id, message=error_message)
    log_scrape_documents.info(f"Job has finished successfully")

def set_document_download_status(
    job_id:str, 
    hash_id:str, 
    scraping_status:ScrapingStatus,
    message:str):
    with sessionmaker() as session:
        existing_record = (
            session.query(DocumentScrapingStatus)
            .filter(
                DocumentScrapingStatus.job_id==job_id,
                DocumentScrapingStatus.hash_id==hash_id)
            .first()
        )
        if not existing_record:
            error_message = (
                f"\nCould not find registered record for"
                f"\nJob id: {job_id}"
                f"\nHash id: {hash_id}"
            )
            raise ValueError(error_message)
        existing_record.scraping_status=scraping_status
        existing_record.message=message
        session.commit()
    return "Successfully updated scraping status"
        
def insert_document_to_db(
    document:KRSDFDocuments) -> str:
    with sessionmaker() as session:
        try:
            session.add(document)
            session.commit()
            return "Successfully added document to db"
        except IntegrityError as e:
            session.rollback()
            warning_message = (
                f"\nIntegrity error has occurred during process of"
                f"\ninserting scraped document into db"
                f"\nMost probably another job has inserted this record first"
                f"\nHash id: {hash_id}"
                f"\nRolling back changes..."
                )
            return warning_message

def set_task_as_failed(
    job_id:str, 
    message:str):
    with sessionmaker() as session:
        existing_records = (
            session.query(DocumentScrapingStatus)
            .filter(
                DocumentScrapingStatus.job_id==job_id)
            .all()
        )
        for record in existing_records:
            record.scraping_status=ScrapingStatus.FAILED
            record.message=message
        session.commit()
        return "Successfully set task status as failed"






            
            











        # log_scrape_documents.debug(f"Scraping document: {hash_id}")
        # log_scrape_documents.debug("Beggining PSQL transaction")
        # with sessionmaker() as session:
        #     log_scrape_documents.debug(f"Fetching information about existing isntances of hash id in DB")
        #     other_instances_of_hash_id = (
        #         session.query(ScrapingCommissions)
        #         .filter(
        #             ScrapingCommissions.hash_id == hash_id,
        #             ScrapingCommissions.job_id != job_id
        #         )
        #         .all()
        #     )
        #     log_scrape_documents.debug(f"Fetching information about current hash id record from DB")
        #     current_record = (
        #         session.query(ScrapingCommissions)
        #         .filter(
        #             ScrapingCommissions.hash_id == hash_id,
        #             ScrapingCommissions.job_id == job_id
        #         )
        #         .first()
        #     )
        #     if other_instances_of_hash_id:
        #         log_scrape_documents.debug(f"Found existing instances of hash id in DB")
        #         log_scrape_documents.debug(f"Checking for jobs with FINISHED status")
        #         finished_jobs_for_hash_id = any(
        #             r.job_status == ScrapingStatus.FINISHED for r in other_instances_of_hash_id
        #         )
        #         log_scrape_documents.debug(f"Checking for jobs with PENDING status")
        #         pending_jobs_hash_ids = any(
        #             r.job_status == ScrapingStatus.PENDING for r in other_instances_of_hash_id
        #         )
        #         log_scrape_documents.debug(f"Checking for jobs with FAILED status")
        #         failed_jobs_hash_ids = any(
        #             r.job_status == ScrapingStatus.FAILED for r in other_instances_of_hash_id
        #         )
        #         if finished_jobs_for_hash_id:
        #             log_scrape_documents.debug(f"Found another record with FINISHED status")
        #             # TODO additional check if record is actually in the DB
        #             log_scrape_documents.debug("Marking current scraping job as finished")
        #             current_record.job_status=ScrapingStatus.FINISHED
        #             current_record.message="Record was already scraped"
        #             session.commit()
        #         elif pending_jobs_hash_ids:
        #             log_scrape_documents.debug(f"Found another record with PENDING status")
        #             try:
        #                 log_scrape_documents.debug(f"Initialising document scraping process")
        #                 data = krsdf.download_documents_scrape_id()
        #                 log_scrape_documents.debug("Trying to insert scraped data into DB")
        #                 scraped_data = ScrapedKrsDF(**data)
        #                 session.add(scraped_data)
        #                 session.flush()
        #             except IntegrityError:
        #                 message = (
        #                     "\nIntegrity Error after trying to insert scraped record into DB"
        #                     "\nMost propably another with PENDING status inserted the row first"
        #                 )
        #                 log_scrape_documents.warning(message)
        #                 session.rollback()
        #                 current_record.job_status=ScrapingStatus.FINISHED
        #                 current_record.message=(message)
        #                 session.commit()
        #             except Exception as e:
        #                 error_message = (
        #                     f"\nError has occurred during scraping process"
        #                     f"\nFailed to scrape document that was already PENDING in another process"
        #                     f"\nDocument that could not be scraped. id: {hash_id}"
        #                     f"\nMarking document scraping status as FAILED"
        #                     f"\nError message: {str(e)}"
        #                 )
        #                 log_scrape_documents.error(error_message)
        #                 current_record.job_status=ScrapingStatus.FAILED
        #                 current_record.message=error_message
        #                 session.commit()
        #             else:
        #                 message = (
        #                     "\nRecord was succesfully inserted, although another job"
        #                     "\nWas scraping it (Another job had PENDING status)"
        #                 )
        #                 log_scrape_documents.debug(message)
        #                 current_record.job_status=ScrapingStatus.FINISHED
        #                 current_record.message=message
        #         elif failed_jobs_hash_ids:
        #             log_scrape_documents.debug(f"Found another record with FAILED status")
        #             try:
        #                 log_scrape_documents.debug(f"Initialising document scraping process")
        #                 data = krsdf.download_documents_scrape_id()
        #             except Exception as e:
        #                 error_message = (
        #                     f"\nError has occurred during scraping process"
        #                     f"\nRe-scraping FAILED record was not successfull"
        #                     f"\nDocument that could not be scraped. id: {hash_id}"
        #                     f"\nMarking document scraping status as FAILED"
        #                     f"\nError message: {str(e)}"
        #                 )
        #                 log_scrape_documents.error(error_message)
        #                 current_record.job_status = ScrapingStatus.FAILED
        #                 current_record.message = error_message
        #                 session.commit()
        #             else:
        #                 log_scrape_documents.debug(f"Successfully re-scraped FAILED document")
        #                 current_record.job_status=ScrapingStatus.FINISHED
        #                 current_record.message="Succesfully re-scraped document"
        #                 scraped_data = ScrapedKrsDF(**data)
        #                 session.commit()
        #     else:
        #         log_scrape_documents.debug("No other instances of document have been found")
        #         try:
        #             log_scrape_documents.debug(f"Initialising document scraping process")
        #             data = krsdf.download_documents_scrape_id()
        #         except Exception as e:
        #             error_message = (
        #                 f"\nError has occurred during scraping process"
        #                 f"\nof new document"
        #                 f"\nDocument could not be scraped. id: {hash_id}"
        #                 f"\nMarking document scraping status as FAILED"
        #                 f"\nError message: {str(e)}"
        #             )
        #             log_scrape_documents.debug(error_message)
        #             current_record.job_status = ScrapingStatus.FAILED
        #             current_record.message = error_message
        #             session.commit()
        #         else:
        #             log_scrape_documents.debug(f"Successfully scraped document")
        #             current_record.job_status=ScrapingStatus.FINISHED
        #             current_record.message="Succesfully scraped document"
        #             scraped_data = ScrapedKrsDF(**data)
        #             session.commit()
