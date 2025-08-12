from fastapi import Request
from fastapi.responses import JSONResponse
import traceback

from config import LOG_LEVEL_POSTGRE_SQL, SOURCE_LOG_SYNC_PSQL_URL
# from business_data_api.utils.logger import setup_logger
from logging_utils import setup_logger

log_to_psql = LOG_LEVEL_POSTGRE_SQL
psql_log_url = SOURCE_LOG_SYNC_PSQL_URL
log = setup_logger(
    logger_name="api_global_exceptions_handler",
    log_to_db=log_to_psql,
    log_to_db_url=psql_log_url)

async def global_exception_handler(
        request:Request,
        e: Exception):
    log.error(
        f"\nUnhandled exception has occured while"
        f"\nprocessing api request"
        f"\nError: {str(e)}"
        f"\nFor url: {request.url}"
        f"\nFull traceback:\n{traceback.format_exc()}"
    )
    return JSONResponse(
        status_code=500,
        content={
            "error":"Internal Server Error",
            "url": str(request.url)
        }
    )