import pandas as pd

from .parse import create_extractors, find_item
from .utils import setup_config, setup_logger


config = setup_config()
logger = setup_logger(__name__, config)


def create_roadmap(lines, roadmap_df, page_map):
    """Create mapping of coordinates and landmarks from CSV and page map."""
    logger.info("Creating roadmap")
    roadmap = pd.concat(
        roadmap_df.apply(
            lambda row: find_item(
                row["landmark"], lines, page_map[row["page"]], row["regex"],
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


def extract_from_roadmap(words, lines, roadmap, extractor_df, page_map):
    extractors = create_extractors(extractor_df, roadmap, page_map)
    return pd.Series(
        extractors.map(lambda extractor: extractor.extract(words, lines)).values,
        index=extractor_df["field_name"]
    )
