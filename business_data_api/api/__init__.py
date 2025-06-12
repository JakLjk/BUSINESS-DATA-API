import os

from flask import Flask
from redis import Redis
from rq import Queue
from dotenv import load_dotenv

from business_data_api.utils.logger import setup_logger

from business_data_api.api.routes.root import bp_root
from business_data_api.api.routes.krs_api import bp_krs_api
from business_data_api.api.routes.krs_dokumenty_finansowe import bp_krs_df




def initialise_flask_api(testing=False):
    api_log = setup_logger(name="api_log")
    api_log.debug("Loading environment variables...")
    load_dotenv()
    redis_url = os.getenv("REDIS_URL")

    api_log.info("Initialising Flask API...")
    app = Flask(__name__)
    if testing:
        app.config["TESTING"] = True
        app.config["DEBUG"] = False

    api_log.info("Setting up Redis connection...")
    redis_conn = Redis.from_url(redis_url)
    api_log.debug("Testing Redis connection...")
    redis_conn.ping()
    api_log.info("Redis connection established successfully.")
    app.redis = redis_conn

    api_log.debug("Registering API blueprints...")
    app.register_blueprint(bp_root)
    app.register_blueprint(bp_krs_api)
    app.register_blueprint(bp_krs_df)

    return app
