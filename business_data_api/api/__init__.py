import os
from fastapi import FastAPI
from redis import Redis
from rq import Queue
from dotenv import load_dotenv
from sqlalchemy import text

from business_data_api.utils.logger import setup_logger
from business_data_api.api.routes.krs_api import router as krs_api_router
from business_data_api.api.routes.krs_dokumenty_finansowe import router as krs_df_router
from business_data_api.db import psql_asession

def create_app(testing:bool = False) -> FastAPI:
    api_log = setup_logger(name="api_log")
    api_log.debug("Loading environment variables...")
    load_dotenv()
    redis_url = os.getenv("REDIS_URL")

    api_log.info("Initialising Fast API...")
    app = FastAPI(title="Business Data API", debug=(not testing))

    api_log.info("Setting up Redis connection...")
    app.state.redis = Redis.from_url(redis_url)
    api_log.debug("Testing Redis connection...")
    app.state.redis.ping()
    api_log.info("Setting up Redis queues...")
    app.state.queues = {
        "KRSDF": Queue("KRSDF", connection=app.state.redis)
    }
    api_log.info("Setting up PostgreSQL connection...")
    app.state.psql_asession = psql_asession

    api_log.debug("Registering API blueprints...")
    app.include_router(krs_api_router, prefix="/krs-api")
    app.include_router(krs_df_router, prefix="/krs-df")

    return app
