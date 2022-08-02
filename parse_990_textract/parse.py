import pandas as pd

from .models import BoundingBox, Extractor
from .utils import get_coordinate, setup_config, setup_logger


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
        left=get_coordinate(roadmap, left_key, "Left", "Left_Default") + left_delta,
        top=get_coordinate(roadmap, top_key, "Top", "Top_Default") + top_delta,
        right=get_coordinate(roadmap, right_key, "Left", "Left_Default") + right_delta,
        bottom=get_coordinate(roadmap, bottom_key, "Top", "Top_Default") + bottom_delta,
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
            regex=row["regex"]
        ),
        axis=1
    )


def find_item(
    landmark, lines, page_no, item_string, default_left,
    default_top, x_tolerance, y_tolerance
):
    found = lines.loc[
        (lines["Page"] == page_no)
        & lines["Text"].str.contains(item_string)
        & lines["Left"].between(
            default_left-x_tolerance, 
            default_left+x_tolerance,
        )
        & lines["Top"].between(
            default_top-y_tolerance,
            default_top+y_tolerance),
        ["Top", "Left"]
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
