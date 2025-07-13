import os
from fastapi import FastAPI
from redis import Redis
from redis.exceptions import ConnectionError
from rq import Queue
from dotenv import load_dotenv
from sqlalchemy import text

from business_data_api.utils.logger import setup_logger
from business_data_api.api.routes.krs_api_services.krs_api import router as krs_api_router
from business_data_api.api.routes.krs_dokumenty_finansowe_services.krs_dokumenty_finansowe import router as krs_df_router
from business_data_api.db import create_async_sessionmaker, create_tables

def create_app(testing:bool = False) -> FastAPI:
    """ 
    Function for initialising FastApi
    """
    # Loading environmental variables
    load_dotenv()
    redis_url = os.getenv("REDIS_URL")
    log_to_psql = bool(os.getenv("LOG_POSTGRE_SQL"))
    psql_log_url = os.getenv("LOG_URL_POSTGRE_SQL")
    psql_async_url = os.getenv("ASYNC_POSTGRESQL_URL")
    psql_sync_url = os.getenv("SYNC_POSTGRE_URL")

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
        "KRSDF": Queue("KRSDF", connection=app.state.redis)
    }
    api_log.debug("Creating missing tables")
    create_tables(psql_sync_url)
    api_log.debug("Setting up PostgreSQL async session")
    app.state.psql_async_sessionmaker = create_async_sessionmaker(psql_async_url)

    api_log.debug("Registering API blueprints")
    app.include_router(krs_api_router, prefix="/krs-api")
    app.include_router(krs_df_router, prefix="/krs-df")

    api_log.info(f"Fast API was successfully intialised")
    return app
