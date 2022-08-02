import logging
import os

from dotenv import dotenv_values


def setup_config(env_file_var="ENVFILE", env_file_default=".env.local"):
    return dotenv_values(os.getenv(env_file_var, env_file_default))


def setup_logger(logger_name, config, log_level_var="PARSE_990_TEXTRACT_OUTPUT_LOG_LEVEL", log_level_default="DEBUG"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(
        getattr(
            logging, config.get(log_level_var, log_level_default)
    )
    return logger
