import os
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import JSONResponse
import traceback

from business_data_api.utils.logger import setup_logger

load_dotenv()
log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
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