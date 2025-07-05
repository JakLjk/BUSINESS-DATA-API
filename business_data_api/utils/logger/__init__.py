import logging
import os

from typing import Optional
from colorlog import ColoredFormatter
from logging.handlers import RotatingFileHandler
from .logging_postgresql_handler import PostgreSQLHandler


def setup_logger(
                logger_name:str,
                stream_logging_level:int = logging.DEBUG,
                # File logging variables
                log_to_file:bool = False,
                log_to_file_logging_level:int = logging.DEBUG,
                log_file_path: Optional[str] = None,
                max_bytes_log_file_size:int = 1 * 1024 * 1024, 
                backup_log_file_count:int = 1,
                # PSQL DB logging variables
                log_to_db:bool= False,
                log_to_db_url:Optional[str] = None,
                log_to_db_logging_level:int = logging.DEBUG
    ) -> logging.Logger:
    """ 
    Function for creating customised logging instances
    Allows for adding handlers that store log data in files
    and for storing data in PostgreSQL table.
    """

    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        return logger
    logger.setLevel(logging.DEBUG)

    formatting = ColoredFormatter(
        fmt="%(log_color)s[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_logging_level)
    stream_handler.setFormatter(formatting)
    logger.addHandler(stream_handler)
    if log_to_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(log_to_file_logging_level)
        file_handler.setFormatter(formatting)
        logger.addHandler(file_handler)
    if log_to_db:
        psql_handler = PostgreSQLHandler(log_to_db_url)
        psql_handler.setLevel(log_to_db_logging_level)
        logger.addHandler(psql_handler)
    return logger