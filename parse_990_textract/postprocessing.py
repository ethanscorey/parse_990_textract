from .utils import setup_config, setup_logger, clean_num


config = setup_config()
logger = setup_logger(__name__, config)


def clean_df(df, non_numeric_columns):
    logger.debug(df)
    logger.debug(df.columns)
    return df.apply(
        lambda x: x.map(clean_num) if not x.name in non_numeric_columns else x,
        axis=0
    ).reset_index().assign(
        split_file=lambda df: df["file"].str.split("_"),
        ein=lambda df: df["split_file"].map(lambda x: x[1]),
        year=lambda df: df["split_file"].map(lambda x: x[3]),
        filing_id=lambda df: df["ein"] + "_" + df["year"],
    )



def clean_filing(df):
    NON_NUMERIC_COLUMNS = [
        "name", "address", "city", "state", "zip", "website",
        "state_of_domicile", "mission", "other_expenses_a_label",
        "other_expenses_b_label", "other_expenses_c_label",
        "other_expenses_d_label", "file",
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
        "specific_type_activity", "file"
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)


def clean_f_ii(df):
    NON_NUMERIC_COLUMNS = [
        "org_name", "irs_code", "region",
        "grant_purpose", "manner_cash", "desc_noncash",
        "method_valuation", "file"
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)


def clean_f_iii(df):
    NON_NUMERIC_COLUMNS = [
        "type_of_grant_assistance", "region",
        "manner_cash_disbursement",
        "desc_noncash_assistance", "file"
    ]
    return clean_df(df, NON_NUMERIC_COLUMNS)
