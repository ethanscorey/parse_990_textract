import sys
from datetime import datetime

import boto3
import pandas as pd


def get_rows_from_db(table, row_filter, attrs, key_filter, **scan_kwargs):
    """Return attributes from DynamoDB table that match filter.

    This assumes that each item in the DB has an attribute named 'documents'.
    """
    last_evaluated = True
    while last_evaluated:
        results = table.scan(**scan_kwargs)
        for item in results["Items"]:
            if row_filter(item):
                for doc in item.get("documents", []):
                    if key_filter(doc):
                        match_entry = {}
                        for attr in attrs:
                            match_entry[attr] = doc.get(attr)
                        yield match_entry
        if last_evaluated_key := results.get("LastEvaluatedKey"):
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
        else:
            last_evaluated = False


def download_990_data(table_name, bucket_name):
    table = boto3.resource("dynamodb").Table(table_name)
    filing_data = []
    sched_f_part_i_data = []
    sched_f_part_ii_data = []
    sched_f_part_iii_data = []
    for row in get_rows_from_db(
        table,
        lambda row: row["doc_type"] == "990",
        (
            "job_id",
            "pdf_key",
            "filing_data",
            "part_i_data",
            "part_ii_data",
            "part_iii_data",
            "source_url",
            "year",
        ),
        lambda doc: (
            not doc.get("error_on_parse", False) and doc.get("job_id")
        ),
    ):
        if row["filing_data"]:
            row["filing_data"] = {
                key: value["0"] for (key, value) in row["filing_data"].items()
            }
            row["filing_data"]["job_id"] = row["job_id"]
            row["filing_data"]["source_url"] = row["source_url"]
            filing_data.append(row["filing_data"])
        if row["part_i_data"]:
            row_part_i_data = pd.DataFrame.from_dict(row["part_i_data"])
            row_part_i_data["job_id"] = row["job_id"]
            row_part_i_data["source_url"] = row["source_url"]
            sched_f_part_i_data.append(row_part_i_data)
        if row["part_ii_data"]:
            row_part_ii_data = pd.DataFrame.from_dict(row["part_ii_data"])
            row_part_ii_data["job_id"] = row["job_id"]
            row_part_ii_data["source_url"] = row["source_url"]
            sched_f_part_ii_data.append(row_part_ii_data)
        if row["part_iii_data"]:
            row_part_iii_data = pd.DataFrame.from_dict(row["part_iii_data"])
            row_part_iii_data["job_id"] = row["job_id"]
            row_part_iii_data["source_url"] = row["source_url"]
            sched_f_part_iii_data.append(row_part_iii_data)
    return {
        "filing_data": pd.DataFrame.from_records(filing_data),
        "sched_f_part_i_data": pd.concat(sched_f_part_i_data).reset_index(
            drop=True
        ),
        "sched_f_part_ii_data": pd.concat(sched_f_part_ii_data).reset_index(
            drop=True
        ),
        "sched_f_part_iii_data": pd.concat(sched_f_part_iii_data).reset_index(
            drop=True
        ),
    }


def main():
    table_name = sys.argv[1]
    bucket_name = sys.argv[2]
    now = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    downloaded = download_990_data(table_name, bucket_name)
    downloaded["filing_data"].set_index("filing_id").to_csv(
        f"output_data/990_filing_data-{now}.csv"
    )
    downloaded["sched_f_part_i_data"].to_csv(
        f"output_data/990_sched_f_part_i_data-{now}.csv", index=False
    )
    downloaded["sched_f_part_ii_data"].to_csv(
        f"output_data/990_sched_f_part_ii_data-{now}.csv", index=False
    )
    downloaded["sched_f_part_iii_data"].to_csv(
        f"output_data/990_sched_f_part_iii_data-{now}.csv", index=False
    )


if __name__ == "__main__":
    main()
