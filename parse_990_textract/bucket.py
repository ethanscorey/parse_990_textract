import io
import json
import logging
import os

import boto3
from dotenv import dotenv_values
import pandas as pd


config = dotenv_values(os.getenv("ENVFILE", ".env.local"))
logger = logging.getLogger(__name__)
logger.setLevel(
    getattr(
        logging, config.get("PARSE_990_TEXTRACT_OUTPUT_LOG_LEVEL", "DEBUG")
    )
)


def get_json(bucket, obj_key):
    json_file = io.BytesIO()
    bucket.download_fileobj(obj_key, json_file)
    json_file.seek(0)
    return json.load(
        io.StringIO(
            json_file.read().decode("utf-8")
        )
    )["Blocks"]


def strip_prefix(key, prefix):
    return key[len(prefix):]


def get_records(
    bucket,
    job_id,
    prefix,
    limit=1000,
    exclude=(".s3_access_check",),
    marker=None
):
    if marker is None:
        logger.info(
            f"Extracting records for job {job_id} from bucket {bucket.name}"
        )
        json_objects = bucket.objects.filter(
            Prefix=f"{prefix}/{job_id}", 
            MaxKeys=limit
        )
    else:
        logger.info(
            f"Extracting records for job {job_id} from bucket {bucket.name}"
            f" starting at object {marker}"
        )
        json_objects = bucket.objects.filter(
            Prefix=f"{prefix}/{job_id}", Marker=marker, MaxKeys=limit
        )
    combined = []
    obj_count = 0
    last_key = ""
    for json_obj in json_objects:
        if strip_prefix(json_obj.key, f"{prefix}/{job_id}/") not in exclude:
            combined.extend(get_json(bucket, json_obj.key))
        obj_count += 1
        last_key = json_obj.key
    logger.info(f"Extracted records from {obj_count} objects.")
    if obj_count > 999:
        combined += get_records(bucket, job_id, marker=last_key)
    return combined


def open_df(bucket, job_id, prefix="textract-output"):
    logger.info(f"Opening dataframe for job {job_id} from bucket {bucket}")
    records = get_records(bucket, job_id, prefix)
    return pd.DataFrame.from_records(
        records,
        index="Id",
        exclude=[
            "ColumnIndex",
            "ColumnSpan",
            "DocumentType",
            "EntityTypes",
            "Hint",
            "Query",
            "SelectionStatus",
            "RowIndex",
            "RowSpan",
        ]
    ).assign(
        Polygon=lambda df: df["Geometry"].map(lambda x: x["Polygon"]),
        Height=lambda df: df["Geometry"].map(lambda x: x["BoundingBox"]["Height"]),
        Left=lambda df: df["Geometry"].map(lambda x: x["BoundingBox"]["Left"]),
        Top=lambda df: df["Geometry"].map(lambda x: x["BoundingBox"]["Top"]),
        Right=lambda df: df["Polygon"].map(
            lambda polygon: max(corner["X"] for corner in polygon)
        ),
        Bottom=lambda df: df["Polygon"].map(
            lambda polygon: max(corner["Y"] for corner in polygon)
        ),
        Midpoint_X=lambda df: (df["Left"] + df["Right"]) / 2,
        Midpoint_Y=lambda df: (df["Top"] + df["Bottom"]) / 2,
        Width=lambda df: df["Geometry"].map(lambda x: x["BoundingBox"]["Width"]),
        Children=lambda df: df["Relationships"].map(lambda x: x[0]["Ids"] if x is not None else x),
        Line_No=lambda df: pd.qcut(df["Top"], 100, labels=list(range(100))).astype(int),
        File=job_id,
    ).sort_values(
        by=["File", "Page", "Line_No", "Left"]
    )
