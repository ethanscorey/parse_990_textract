import json
import logging

import boto3
from dotenv import dotenv_values

from .bucket import open_df
from .utils import setup_config, setup_logger


config = setup_config()
logger = setup_logger(__name__, config)


def handler(event, context):
    logger.info("## EVENT DATA")
    logger.info(event)
    bucket = boto3.resource("s3").Bucket("s3-ocr-990s-demo")
    df = open_df(bucket, "00057f8f88b60da8bb6be94dc1f3da3012d0c139f460184bd6f459bee76df97a")
    extractor_df = "foo"
    roadmap_df = "bar"
    schedule_f_tablemap_df = "baz"
    schedule_f_table_extractor_df = "bang"
    schedule_f_row_extractor = "fizz"
    return {
        "statusCode": 200,
        "body": json.dumps(df.shape)
    }
