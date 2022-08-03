import pandas as pd

from .models import TableExtractor
from .parse import find_item, find_table_pages
from .utils import setup_config, setup_logger


config = setup_config()
logger = setup_logger(__name__, config)


def create_tablemap(lines, tablemap_df, page):
    roadmap = pd.concat(
        tablemap_df.apply(
            lambda row: find_item(
                row["landmark"], lines, page, row["regex"],
                row["left_default"], row["top_default"],
                row["x_tolerance"], row["y_tolerance"]
            ).assign(
                Item=row["landmark"],
                Top_Default=row["top_default"],
                Left_Default=row["left_default"],
            ),
            axis=1
        ).values
    )
    return pd.concat([
        roadmap,
        pd.DataFrame({
            "Item": ["Top Left Corner", "Bottom Right Corner"],
            "Top": [0, 1],
            "Left": [0, 1],
            "Top_Default": [0, 1],
            "Left_Default": [0, 1],
        }),
    ]).set_index("Item")


def extract_table_data(
    pages, lines, words, header, table_name, 
    tablemap_df, table_extractor_df, row_extractor_df
):
    table_pages = find_table_pages(
        pages["Text"].agg(lambda words: " ".join(words)),
        header,
    )
    tablemaps = pd.DataFrame(
        {
            "page": table_pages,
            "tablemap": table_pages.map(
                lambda page: create_tablemap(lines, tablemap_df, page).dropna()
            ),
        }
    )
    table_row_extractors = row_extractor_df.loc[
        row_extractor_df["table"] == table_name
    ]
    table = table_extractor_df.loc[
        table_extractor_df["table"] == table_name
    ].iloc[0]
    try:
        rows = tablemaps.assign(
            extractor=tablemaps["tablemap"].map(
                lambda tablemap: TableExtractor(
                    top_label=table["table_top"],
                    top_delta=table["table_top_delta"],
                    bottom_label=table["table_bottom"],
                    bottom_delta=table["table_bottom_delta"],
                    row_margin=table["row_margin"],
                    index_col_left_label=table["index_col_left"],
                    index_col_left_delta=table["index_col_left_delta"],
                    index_col_right_label=table["index_col_right"],
                    index_col_right_delta=table["index_col_right_delta"],
                    tablemap=tablemap,
                    row_extractors=table_row_extractors,
                    fields=table_row_extractors["field"],
                ),
            ),
        ).apply(
            lambda row: row["extractor"].extract_rows(words, row["page"]),
            axis=1
        ).dropna()
    except KeyError as e:
        logger.error(f"{type(e)}: {e}")
    else:
        if rows.count().any():
            return pd.concat(rows.values).reset_index(drop=True)
