import pandas as pd

from .parse import create_extractors, find_item
from .utils import setup_config, setup_logger, sort_words

config = setup_config()
logger = setup_logger(__name__, config)


def create_roadmap(lines, roadmap_df, page_map):
    """Create mapping of coordinates and landmarks from CSV and page map."""
    logger.info("Creating roadmap")
    roadmap = pd.concat(
        roadmap_df.apply(
            lambda row: find_item(
                row["landmark"],
                lines,
                page_map[row["page"]],
                row["regex"],
                row["left_default"],
                row["top_default"],
                row["x_tolerance"],
                row["y_tolerance"],
            ).assign(
                Item=row["landmark"],
                Top_Default=row["top_default"],
                Left_Default=row["left_default"],
            ),
            axis=1,
        ).values
    )
    return pd.concat(
        [
            roadmap,
            pd.DataFrame(
                {
                    "Item": ["Top Left Corner", "Bottom Right Corner"],
                    "Top": [0, 1],
                    "Left": [0, 1],
                    "Top_Default": [0, 1],
                    "Left_Default": [0, 1],
                }
            ),
        ]
    ).set_index("Item")


def extract_from_roadmap(words, lines, roadmap, extractor_df, page_map):
    page_words = {
        page_no: words.loc[index].assign(WordIndex=lambda df: sort_words(df))
        for (page_no, index) in words.groupby("Page").groups.items()
    }
    page_lines = {
        page_no: lines.loc[index].assign(WordIndex=lambda df: sort_words(df))
        for (page_no, index) in lines.groupby("Page").groups.items()
    }
    extractors = create_extractors(extractor_df, roadmap, page_map)
    return pd.Series(
        extractors.map(
            lambda extractor: extractor.extract(page_words, page_lines)
        ).values,
        index=extractor_df["field_name"],
    )
