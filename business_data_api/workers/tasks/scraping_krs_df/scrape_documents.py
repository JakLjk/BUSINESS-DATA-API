import os
from dotenv import load_dotenv
from redis import Redis
from sqlalchemy.exc import IntegrityError

from business_data_api.utils.logger import setup_logger
from business_data_api.db import create_sync_sessionmaker
from business_data_api.scraping.krs_dokumenty_finansowe.model import KRSDokumentyFinansowe
from business_data_api.db.models import KRSDFDocuments

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
psql_sync_url = os.getenv("SYNC_POSTGRE_URL")
redis_url = os.getenv("REDIS_URL")
stale_job_treshold_seconds = os.getenv("STALE_JOB_TRESHOLD_SECONDS")
sessionmaker = create_sync_sessionmaker(psql_sync_url)
redis_conn = Redis.from_url(redis_url)


def task_scrape_documents(job_id:str, krs:str):
    """
    Scrape documents that are not available in local DB
    """
    log = setup_logger(
        logger_name=f"worker_scrape_krs_df_documents",
        logger_id=job_id,
        log_to_db=log_to_psql,
        log_to_db_url=psql_log_url
        )
    log.info(f"Starting process of scraping documents for krs {krs}")
    log.debug(f"Starting DB session")
    log.debug(f"Fetching information about locally avaiable documents")
    with sessionmaker() as session:
        locally_available_documents = (
            session.query(KRSDFDocuments)
            .filter(KRSDFDocuments.krs_number==krs)
            .all()
        )
    available_hash_ids = [r.hash_id for r in locally_available_documents]
    log.debug(f"There are {len(available_hash_ids)} documents available locally")
    log.debug(f"Initialising scraper object")
    try:
        krsdf = KRSDokumentyFinansowe(krs)
        krsdf.download_documents(
            document_hash_id_s_to_omit = available_hash_ids
        )
    except Exception as e:
        log.error(
            f"\nException has occured while trying to"
            f"\nInitialise scraper object"
            f"\nException: {str(e)}")
        raise e
    log.debug("Starting scraping process")
    while hash_id := krsdf.download_documents_next_id_value():
        log.debug(f"Scraping hash id {hash_id}")
        try:
            document = krsdf.download_documents_scrape_id()
        except Exception as e:
            log.error(
                f"\nException has occured during scraping"
                f"\nprocess for hash_id: {hash_id}"
                f"\nException: {str(e)}")
            raise e
        data_row = KRSDFDocuments(**document)
        log.debug(f"Inserting hash id into database")
        with sessionmaker() as session:
            try:
                session.add(data_row)
                session.commit()
            except IntegrityError:
                session.rollback()
                log.warning(
                    f"\nIntegrity error has occurred while committing"
                    f"\ndocument to the DB"
                    f"\nMaybe another process has added this record"
                    f"\nin the time of this session (race condition)"
                    f"\nError: {str(e)}")
                # TODO check if record is in db actually, if not raise additional error
            except Exception as e:
                log.error(
                    f"\nException has occured while committing"
                    f"\nDocument to the DB"
                    f"\nException: {str(e)}"
                )
                raise e