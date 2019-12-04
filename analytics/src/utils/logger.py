"""
creates root loggers with default file handlers & log levels
"""

import logging


def get_logger(log_path):
    """
    Creates an Error Logger
    """

    log_handler = logging.FileHandler(log_path)

    log_handler.setFormatter(logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s'))

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.addHandler(log_handler)

    return logger
