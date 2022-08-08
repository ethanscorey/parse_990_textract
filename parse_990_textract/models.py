import dataclasses
import re

import pandas as pd

from .utils import (
    cluster_words, get_best_match, get_coordinate,
    get_regex, setup_config, setup_logger
)


config = setup_config()
logger = setup_logger(__name__, config)


@dataclasses.dataclass
class BoundingBox:
    left: int
    left_delta: int
    top: int
    top_delta: int
    right: int
    right_delta: int
    bottom: int
    bottom_delta: int

    def get_text_in_box(self, text, page_no):
        text_in_box = text.loc[
            text["Midpoint_X"].between(
                self.left + self.left_delta, 
                self.right + self.right_delta
            )
            & text["Midpoint_Y"].between(
                self.top + self.top_delta,
                self.bottom + self.bottom_delta
            )
            & (text["Page"] == page_no),
            "Text"
        ].agg(lambda x: " ".join(x.values))
        if not any(text_in_box):
            return ""
        return text_in_box
        
        
@dataclasses.dataclass
class Extractor:
    name: str
    strategy: str
    page: int
    bounding_box: BoundingBox
    regex: re.Pattern
        
    def extract(self, words, lines):
        if self.strategy == "words":
            words_in_box = self.bounding_box.get_text_in_box(
                words,
                self.page,
            )
        elif self.strategy == "lines":
            words_in_box = self.bounding_box.get_text_in_box(
                lines,
                self.page,
            )
        if not any(words_in_box):
            return ""
        result = get_regex(words_in_box, self.regex, "match", "NO MATCH")
        if result == "NO MATCH":
            logger.error(f"No match for {self.name} in {words_in_box}")
            return ""
        return result


@dataclasses.dataclass
class TableExtractor:
    top_label: str
    top_delta: float
    bottom_label: str
    bottom_delta: float
    tablemap: pd.DataFrame
    row_extractors: pd.DataFrame
    fields: list[str]
        
        
    @property
    def table_top(self):
        return get_coordinate(
            self.tablemap, self.top_label,
            "Top", "Top_Default"
        ) + self.top_delta
    
    @property
    def table_bottom(self):
        try:
            return get_coordinate(
                self.tablemap, self.bottom_label,
                "Top", "Top_Default",
            ) + self.bottom_delta
        except KeyError:
            return .99
        
    def get_col_span(self, col_left, col_right):
        col_span = (
            get_coordinate(
                self.tablemap, col_left,
                "Left", "Left_Default"
            ),
            get_coordinate(
                self.tablemap, col_right,
                "Left", "Left_Default"
            ),
        )
        return col_span
    
    def get_row_spans(self, words, page):
        table_words = words.loc[
            (words["Page"] == page)
            & words["Midpoint_Y"].between(
                self.table_top,
                self.table_bottom,
            )
        ]
        y_tol = table_words["Height"].max() * 1.1
        x_tol = (
            (table_words["Right"].max() - table_words["Left"].min())
            / len(self.fields)
        )
        word_clusters = cluster_words(
            table_words,
            table_words["Height"].min(),
            "Midpoint_Y",
        )
        last_cluster = {
            "Left": word_clusters[0]["Left"].min(),
            "Right": word_clusters[0]["Right"].max(),
            "Midpoint_Y": word_clusters[0]["Midpoint_Y"].median(),
            "Top": word_clusters[0]["Top"].min(),
            "Bottom": word_clusters[0]["Bottom"].max(),
        }
        row_tops = [self.table_top]
        row_bottoms = []
        state = "IN_ROW"
        
        for cluster in word_clusters[1:]:
            cluster_coords = {
                "Left": cluster["Left"].min(),
                "Right": cluster["Right"].max(),
                "Midpoint_Y": cluster["Midpoint_Y"].median(),
                "Top": cluster["Top"].min(),
                "Bottom": cluster["Bottom"].min(),
            }
            left_delta = last_cluster["Left"] - cluster_coords["Left"]
            right_delta = last_cluster["Right"] - cluster_coords["Right"]
            y_delta = cluster_coords["Midpoint_Y"] - last_cluster["Midpoint_Y"]
            if state == "START_ROW":
                row_tops.append(cluster_coords["Top"])
                state = "IN_ROW"
            else:
                if (y_delta > y_tol) or (left_delta > x_tol):
                    row_tops.append(cluster_coords["Top"])
                    row_bottoms.append(last_cluster["Bottom"])
                elif right_delta > x_tol:
                    row_bottoms.append(cluster_coords["Bottom"])
                    state = "START_ROW"
            last_cluster = cluster_coords
        if len(row_bottoms) < len(row_tops):
            row_bottoms.append(self.table_bottom)
        return pd.DataFrame(
            {
                "row_top": row_tops,
                "row_bottom": row_bottoms,
            }
        )
    
    def extract_rows(self, words, page):
        rows = list(
                row := self.get_row_spans(words, page).apply(
                    lambda row: self.extract_row(words, page, (row["row_top"], row["row_bottom"])).values,
                    axis=1,
                ).values
            )
        non_empty_rows = [row for row in rows if row.any()]
        if non_empty_rows:
            return pd.DataFrame(non_empty_rows, columns=self.fields)
        return pd.NA
        
    def extract_row(self, words, page, row_span):
        row = self.row_extractors.apply(
            lambda field: self.get_cell_value(
                words, page, field, row_span
            ),
            axis=1,
        )
        return row

    def get_cell_value(self, words, page, field, row_span):
        col_span = self.get_col_span(
            field["col_left"], field["col_right"]
        )
        row_width = row_span[1] - row_span[0]
        bounding_box = BoundingBox(
            left=col_span[0],
            left_delta=field["left_delta"],
            top=row_span[0],
            top_delta=-0.1*row_width,
            right=col_span[1],
            right_delta=field["right_delta"],
            bottom=row_span[1],
            bottom_delta=0.1*row_width,
        )
        return bounding_box.get_text_in_box(words, page)
