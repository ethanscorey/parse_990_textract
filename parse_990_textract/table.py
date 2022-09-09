import pandas as pd

from .models import TableExtractor
from .parse import find_item, find_table_pages
from .utils import setup_config, setup_logger


config = setup_config()
logger = setup_logger(__name__, config)


def create_tablemap(lines, tablemap_df, page, table_name):
    roadmap = pd.concat(
        tablemap_df.loc[
            tablemap_df["table"] == table_name
        ].apply(
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
                lambda page: create_tablemap(lines, tablemap_df, page, table_name)
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
                    header_top_label=table["header_top"],
                    top_label=table["table_top"],
                    bottom_label=table["table_bottom"],
                    tablemap=tablemap,
                    fields=table_row_extractors["field"].reset_index(
                        drop=True
                    ),
                    field_labels=table_row_extractors["col_left"].reset_index(
                        drop=True
                    ),
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
