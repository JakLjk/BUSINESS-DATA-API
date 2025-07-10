import os
from dotenv import load_dotenv

from business_data_api.utils.logger import setup_logger
from business_data_api.tasks.krs_dokumenty_finansowe.get_krs_df import KRSDokumentyFinansowe
from business_data_api.workers.tasks.status import ScrapingStatus

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")


def task_get_document_list(job_id:str, krs:str):
    log_document_list_worker = setup_logger(
    logger_name=f"worker_task_get_document_names",
    logger_id=job_id,
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url
    )
    log_document_list_worker.info(
        f"Scraping infromation about document list"
        f"for krs number: {krs}")
    try:
        krsdf = KRSDokumentyFinansowe(krs)
        documents_list = krsdf.get_document_list()
        log_document_list_worker.debug(
            f"Returning infromation about document list"
            f"for krs number: {krs}")
        return {
            "status":ScrapingStatus.SUCCESS,
            "document_list":documents_list
        }
    except Exception as e:
        error_message = (
            f"\nException has occured while scraping document list"
            f"\nfor krs: {krs}"
            f"\nJob id:{job_id}"
            f"\nError: {str(e)}"
        )
        log_document_list_worker.error(error_message)
        return {
            "status":ScrapingStatus.FAILED,
            "error_message":str(e)
        }
