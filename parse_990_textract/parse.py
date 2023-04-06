import pandas as pd

from .models import BoundingBox, Extractor
from .utils import get_coordinate, setup_config, setup_logger

config = setup_config()
logger = setup_logger(__name__, config)


def find_pages(ocr_data):
    return {
        "Page 1": id_first_page(ocr_data),
        "Page 9": id_page_9(ocr_data),
        "Page 10": id_page_10(ocr_data),
        "Schedule F, Page 1": (sched_f := id_sched_f(ocr_data)),
        "Schedule F, Page 2": sched_f + 1 if sched_f else sched_f,
    }


def id_sched_f(ocr_data):
    matching_page = ocr_data.loc[
        ocr_data["Text"].str.contains(
            "General Information on Activities Outside"
        ),
        "Page",
    ]
    if not matching_page.count():
        return 0
    return matching_page.iloc[0]


def id_page_9(ocr_data):
    matching_page = ocr_data.loc[
        ocr_data["Text"].str.contains("Statement of Revenue"),
        "Page",
    ]
    if not matching_page.count():
        logger.error("Statement of revenue missing.")
        return 0
    return matching_page.iloc[0]


def id_page_10(ocr_data):
    matching_page = ocr_data.loc[
        ocr_data["Text"].str.contains("Statement of Functional Expenses"),
        "Page",
    ]
    if not matching_page.count():
        logger.error("Statement of functional expenses missing")
        return 0
    return matching_page.iloc[0]


def id_first_page(ocr_data):
    by_page = ocr_data.groupby("Page")
    if by_page["Top"].max().iloc[0] < 0.7:
        return 2
    return 1


def create_bounding_box(
    roadmap,
    left_key,
    left_delta,
    top_key,
    top_delta,
    right_key,
    right_delta,
    bottom_key,
    bottom_delta,
):
    return BoundingBox(
        left=get_coordinate(roadmap, left_key, "Left", "Left_Default"),
        left_delta=left_delta,
        top=get_coordinate(roadmap, top_key, "Top", "Top_Default"),
        top_delta=top_delta,
        right=get_coordinate(roadmap, right_key, "Left", "Left_Default"),
        right_delta=right_delta,
        bottom=get_coordinate(roadmap, bottom_key, "Top", "Top_Default"),
        bottom_delta=bottom_delta,
    )


def create_extractors(extractor_df, roadmap, page_map):
    return extractor_df.apply(
        lambda row: Extractor(
            name=row["field_name"],
            strategy=row["strategy"],
            page=page_map[row["page"]],
            bounding_box=create_bounding_box(
                roadmap,
                row["left"],
                row["left_delta"],
                row["top"],
                row["top_delta"],
                row["right"],
                row["right_delta"],
                row["bottom"],
                row["bottom_delta"],
            ),
            regex=row["regex"],
        ),
        axis=1,
    )


def find_item(
    landmark,
    lines,
    page_no,
    item_string,
    default_left,
    default_top,
    x_tolerance,
    y_tolerance,
):
    found = lines.loc[
        (lines["Page"] == page_no)
        & lines["Text"].str.contains(item_string)
        & lines["Left"].between(
            default_left - x_tolerance,
            default_left + x_tolerance,
        )
        & lines["Top"].between(
            default_top - y_tolerance, default_top + y_tolerance
        ),
        ["Top", "Left"],
    ].reset_index()
    found = found.drop(columns=["Id"])
    if found["Top"].count() < 1:
        found = pd.DataFrame(
            {
                "Top": [pd.NA],
                "Left": [pd.NA],
            }
        )
    return found.iloc[:1]


def find_table_pages(page_text, table_header):
    return page_text.loc[
        page_text.str.contains(table_header)
    ].index.to_series()
