import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

from colorlog import ColoredFormatter

def setup_logger(
                name:str,
                level: int = logging.DEBUG,
                log_file: Optional[str] = None,
                max_bytes: int = 1 * 1024 * 1024, 
                backup_count: int = 1,
    ) -> logging.Logger:

    logger = logging.getLogger(name)
    if logger.hasHandlers():
        raise Exception(
            f"Logger '{name}' already has handlers. Please remove existing handlers before setting up a new logger."
        )

    logger.setLevel(level)

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
    stream_handler.setFormatter(formatting)
    logger.addHandler(stream_handler)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setFormatter(formatting)
        logger.addHandler(file_handler)
    return logger