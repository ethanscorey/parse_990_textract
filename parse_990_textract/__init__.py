import json
import logging

import boto3
from dotenv import dotenv_values
import pandas as pd

from .bucket import open_df
from .filing import create_roadmap, extract_from_roadmap
from .parse import find_pages
from .postprocessing import clean_filing, clean_f_i, clean_f_ii, clean_f_iii
from .setup import load_extractor_df
from .table import extract_table_data
from .utils import setup_config, setup_logger


config = setup_config()
logger = setup_logger(__name__, config)


def handler(event, context):
    logger.info("## EVENT DATA")
    logger.info(event)

    extractor_df = load_extractor_df("990_extractors.csv")
    roadmap_df = pd.read_csv("990_roadmap.csv")
    schedule_f_tablemap_df = pd.read_csv("schedule_f_table_roadmap.csv")
    schedule_f_table_extractor_df = pd.read_csv("schedule_f_table_extractors.csv")
    schedule_f_row_extractor_df = pd.read_csv("schedule_f_row_extractors.csv")

    PART_I_HEADER = r"\(a\)\s*Region|\(d\)\s*Activities|\(e\)\s*If activity|\(f\)Total expenditures"
    PART_II_HEADER = r"\(b\)\s*IRS code|\(c\)\s*Region|\(d\)\s*Purpose|\(f\)\s*Manner|\(h\)\s*Description"
    PART_III_HEADER = r"\(b\)\s*Region|\(e\)\s*Manner of cash|\(h\)\s*Method of va"
    PART_I_TABLE_NAME = "Activities per Region"
    PART_II_TABLE_NAME = r"Grants to Organizations Outside the United States"
    PART_III_TABLE_NAME = "Grants to Individuals Outside the United States"

    bucket = boto3.resource("s3").Bucket(event.get("bucket_name"))
    data = open_df(bucket, event.get("textract_job_id"))

    lines = data.loc[data["BlockType"] == "LINE"]
    words = data.loc[data["BlockType"] == "WORD"]
    pages = lines.groupby("Page")
    page_map = find_pages(lines)
    roadmap = create_roadmap(
        lines, roadmap_df, page_map
    )

    row = extract_from_roadmap(
        words, lines, roadmap, extractor_df, page_map
    )
    row["file"] = event.get("pdf_key")
    row["job_id"] = event.get("textract_job_id")
    row = clean_filing(row.to_frame().T)

    part_i_table = extract_table_data(
        pages, lines, words, PART_I_HEADER, PART_I_TABLE_NAME,
        schedule_f_tablemap_df, schedule_f_table_extractor_df,
        schedule_f_row_extractor_df,
    )
    if part_i_table is not None:
        part_i_table["file"] = event.get("pdf_key")
        part_i_table["job_id"] = event.get("textract_job_id")
        part_i_table = clean_f_i(part_i_table)
        part_i_table = part_i_table.to_dict()

    part_ii_table = extract_table_data(
        pages, lines, words, PART_II_HEADER, PART_II_TABLE_NAME,
        schedule_f_tablemap_df, schedule_f_table_extractor_df,
        schedule_f_row_extractor_df,
    )
    if part_ii_table is not None:
        part_ii_table["file"] = event.get("pdf_key")
        part_ii_table["job_id"] = event.get("textract_job_id")
        part_ii_table = clean_f_ii(part_ii_table)
        part_ii_table = part_ii_table.to_dict()

    part_iii_table = extract_table_data(
        pages, lines, words, PART_III_HEADER, PART_III_TABLE_NAME,
        schedule_f_tablemap_df, schedule_f_table_extractor_df,
        schedule_f_row_extractor_df,
    )
    if part_iii_table is not None:
        part_iii_table["file"] = event.get("pdf_key")
        part_iii_table["job_id"] = event.get("textract_job_id")
        part_iii_table = clean_f_iii(part_iii_table)
        part_iii_table = part_iii_table.to_dict()
    
    return {
        "statusCode": 200,
        "body": {
            "filing_data": json.dumps(row.to_dict()),
            "part_i_data": json.dumps(part_i_table),
            "part_ii_data": json.dumps(part_ii_table),
            "part_iii_data": json.dumps(part_iii_table),
            "ein": event.get("ein"),
            "doc_type": event.get("doc_type"),
            "pdf_key": event.get("pdf_key"),
            "table_name": event.get("table_name"),
        }
    }
