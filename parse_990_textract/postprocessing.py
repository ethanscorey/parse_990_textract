import pandas as pd

from .utils import setup_config, setup_logger, clean_num


config = setup_config()
logger = setup_logger(__name__, config)


def postprocess(data, job_id, pdf_key, clean_func):
    if data is not None:
        data["job_id"] = job_id
        data["pdf_key"] = pdf_key
        if isinstance(data, pd.Series):
            data = data.to_frame().T
        return clean_func(data)


def clean_df(df, non_numeric_columns):
    return df.apply(
        lambda x: x.map(clean_num) if not x.name in non_numeric_columns else x,
        axis=0
    ).reset_index(drop=True).assign(
        split_pdf_key=lambda df: df["pdf_key"].str.split("_"),
        ein=lambda df: df["split_pdf_key"].map(lambda x: x[1]),
        year=lambda df: df["split_pdf_key"].map(lambda x: x[3]),
        filing_id=lambda df: df["ein"] + "_" + df["year"],
    ).drop(columns=["split_pdf_key"])



def clean_filing(df):
    NON_NUMERIC_COLUMNS = [
        "name", "address", "city", "state", "zip", "website",
        "state_of_domicile", "mission", "other_expenses_a_label",
        "other_expenses_b_label", "other_expenses_c_label",
        "other_expenses_d_label", "pdf_key", "job_id",
        "activities_per_region_subtotal_activities_conducted",
        "activities_per_region_subtotal_specific_type", 
        "activities_per_region_continuation_total_activities_conducted",
        "activities_per_region_continuation_total_specific_type",
        "activities_per_region_totals_activities_conducted",
        "activities_per_region_totals_specific_type",
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)


def clean_f_i(df):
    NON_NUMERIC_COLUMNS = [
        "region", "activities_conducted",
        "specific_type_activity", "pdf_key", "job_id",
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)


def clean_f_ii(df):
    NON_NUMERIC_COLUMNS = [
        "org_name", "irs_code", "region",
        "grant_purpose", "manner_cash", "desc_noncash",
        "method_valuation", "pdf_key", "job_id",
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)


def clean_f_iii(df):
    NON_NUMERIC_COLUMNS = [
        "type_of_grant_assistance", "region",
        "manner_cash_disbursement", "job_id",
        "desc_noncash_assistance", "pdf_key"
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)
