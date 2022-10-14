import json

import boto3
import pandas as pd

from .bucket import open_df
from .filing import create_roadmap, extract_from_roadmap
from .parse import find_pages
from .postprocessing import (
    clean_f_i,
    clean_f_ii,
    clean_f_iii,
    clean_filing,
    postprocess,
)
from .setup import load_extractor_df
from .table import extract_table_data
from .utils import setup_config, setup_logger

config = setup_config()
logger = setup_logger(__name__, config)


def handler(event, context):
    logger.info("## EVENT DATA")
    logger.info(event)

    bucket_name = event.get("bucket_name")
    job_id = event.get("textract_job_id")
    pdf_key = event.get("pdf_key")

    extractor_df = load_extractor_df("parse_data/990_extractors.csv")
    roadmap_df = pd.read_csv("parse_data/990_roadmap.csv")
    schedule_f_tablemap_df = pd.read_csv(
        "parse_data/schedule_f_table_roadmap.csv"
    )
    schedule_f_table_extractor_df = pd.read_csv(
        "parse_data/schedule_f_table_extractors.csv"
    )
    schedule_f_row_extractor_df = pd.read_csv(
        "parse_data/schedule_f_row_extractors.csv"
    )

    PART_I_HEADER = (
        r"\(a\)\s*Region|\(d\)\s*Activities|\(e\)\s*"
        r"If activity|\(f\)Total expenditures"
    )
    PART_II_HEADER = (
        r"\(b\)\s*IRS code|\(c\)\s*Region|\(d\)\s*"
        r"Purpose|\(f\)\s*Manner|\(h\)\s*Description"
    )
    PART_III_HEADER = (
        r"\(b\)\s*Region|\(e\)\s*Manner of cash|\(h\)\s*Method of va"
    )
    PART_I_TABLE_NAME = "Activities per Region"
    PART_II_TABLE_NAME = r"Grants to Organizations Outside the United States"
    PART_III_TABLE_NAME = "Grants to Individuals Outside the United States"

    bucket = boto3.resource("s3").Bucket(bucket_name)
    data = open_df(bucket, job_id)

    lines = data.loc[data["BlockType"] == "LINE"]
    words = data.loc[data["BlockType"] == "WORD"]
    pages = lines.groupby("Page")
    page_map = find_pages(lines)
    if lines.loc[
        (lines["Page"] == page_map["Page 1"])
        & lines["Text"].str.contains(
            "Net rental income|Direct public|IRS label"
        ),
        "Page",
    ].any():
        raise ValueError("Incorrect form version.")
    roadmap = create_roadmap(lines, roadmap_df, page_map)

    row = extract_from_roadmap(words, lines, roadmap, extractor_df, page_map)
    row = postprocess(row, job_id, pdf_key, clean_filing)

    part_i_table = extract_table_data(
        pages,
        lines,
        words,
        PART_I_HEADER,
        PART_I_TABLE_NAME,
        schedule_f_tablemap_df,
        schedule_f_table_extractor_df,
        schedule_f_row_extractor_df,
    )
    part_i_table = postprocess(part_i_table, job_id, pdf_key, clean_f_i)
    if part_i_table is not None:
        part_i_table = part_i_table.to_dict()

    part_ii_table = extract_table_data(
        pages,
        lines,
        words,
        PART_II_HEADER,
        PART_II_TABLE_NAME,
        schedule_f_tablemap_df,
        schedule_f_table_extractor_df,
        schedule_f_row_extractor_df,
    )
    part_ii_table = postprocess(part_ii_table, job_id, pdf_key, clean_f_ii)
    if part_ii_table is not None:
        part_ii_table = part_ii_table.to_dict()

    part_iii_table = extract_table_data(
        pages,
        lines,
        words,
        PART_III_HEADER,
        PART_III_TABLE_NAME,
        schedule_f_tablemap_df,
        schedule_f_table_extractor_df,
        schedule_f_row_extractor_df,
    )
    part_iii_table = postprocess(part_iii_table, job_id, pdf_key, clean_f_iii)
    if part_iii_table is not None:
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
            "pdf_key": pdf_key,
            "bucket_name": bucket_name,
            "table_name": event.get("table_name"),
        },
    }
