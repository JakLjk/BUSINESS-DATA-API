from fastapi import FastAPI
from redis import Redis
from redis.exceptions import ConnectionError
from rq import Queue
from dotenv import load_dotenv
from sqlalchemy import text

from config import (
    REDIS_URL, 
    LOG_TO_POSTGRE_SQL, 
    SOURCE_LOG_SYNC_PSQL_URL,
    SOURCE_ASYNC_PSQL_URL,
    SOURCE_SYNC_PSQL_URL)
# from business_data_api.utils.logger import setup_logger
from logging_utils import setup_logger
from business_data_api.api.routes.root.root import router as root_router
from business_data_api.api.routes.krs_api_services.krs_api import router as krs_api_router
from business_data_api.api.routes.krs_dokumenty_finansowe_services.krs_dokumenty_finansowe import router as krs_df_router
from business_data_api.api.routes.exception_handlers.handlers import global_exception_handler
from business_data_api.db import create_async_sessionmaker, create_tables

def create_app(testing:bool = False) -> FastAPI:
    """ 
    Function for initialising FastApi
    """
    # Loading environmental variables
    redis_url = REDIS_URL
    log_to_psql = LOG_TO_POSTGRE_SQL
    psql_log_url = SOURCE_LOG_SYNC_PSQL_URL
    psql_async_url = SOURCE_ASYNC_PSQL_URL
    psql_sync_url = SOURCE_SYNC_PSQL_URL

    api_log = setup_logger(
        logger_name="fast_api_main",
        log_to_db=log_to_psql,
        log_to_db_url=psql_log_url
        )
    # FIXME currently not working
    uvicorn_log = setup_logger(
        logger_name="uvicorn.access",
        log_to_db=log_to_psql,
        log_to_db_url=psql_log_url
    )
    api_log.info("Initialising Fast API")
    api_log.debug(f"Testing status: {testing}")
    app = FastAPI(title="Business Data API", debug=(not testing))

    api_log.debug("Setting up Redis connection")
    app.state.redis = Redis.from_url(redis_url)
    api_log.debug("Testing Redis connection")
    try:
        app.state.redis.ping()
    except ConnectionError as e:
        api_log.error(
            f"Connection attempt to Redis has failed. "
            f"Redis URL {redis_url}")
        raise e
    api_log.debug("Setting up Redis queues")
    app.state.queues = {
        "KRSDF": Queue("KRSDF", connection=app.state.redis),
        "KRSAPI": Queue("KRSAPI", connection=app.state.redis)
    }
    api_log.debug("Creating missing tables")
    create_tables(psql_sync_url)
    api_log.debug("Setting up PostgreSQL async session")
    app.state.psql_async_sessionmaker = create_async_sessionmaker(psql_async_url)

    api_log.debug(f"Registering exception handlers")
    app.add_exception_handler(Exception, global_exception_handler)
    api_log.debug("Registering API blueprints")
    app.include_router(root_router, prefix="/data")
    app.include_router(krs_api_router, prefix="/krs-api")
    app.include_router(krs_df_router, prefix="/krs-df")

    api_log.info(f"Fast API was successfully intialised")
    return app
