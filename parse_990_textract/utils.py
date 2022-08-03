import logging
import math
import os

from dotenv import dotenv_values
import pandas as pd


def setup_config(env_file_var="ENVFILE", env_file_default=".env.local"):
    return dotenv_values(os.getenv(env_file_var, env_file_default))


def setup_logger(logger_name, config, log_level_var="PARSE_990_TEXTRACT_OUTPUT_LOG_LEVEL", log_level_default="DEBUG"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(
        getattr(
            logging, config.get(log_level_var, log_level_default)
        )
    )
    return logger


def trunc_num(value, places):
    return math.trunc(value * 10**places) / 10**places


def get_regex(string, regex, match_group=0, alt_value=None):
    match = regex.search(string)
    if match is not None:
        return match.group(match_group)
    return alt_value


def get_best_match(string, regex, alt_value=None):
    match = regex.search(string)
    if match is not None:
        return max(
            match.groups(),
            key=lambda x: len(x) if x is not None else 0
        )
    return alt_value


def get_coordinate(roadmap, index, col, default):
    value = roadmap.loc[index, col]
    if pd.isna(value):
        value = roadmap.loc[index, default]
    return trunc_num(value, 2)
