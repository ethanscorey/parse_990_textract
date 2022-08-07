import dataclasses
import re

import pandas as pd

from .utils import (
    get_best_match, get_coordinate, get_regex,
    setup_config, setup_logger
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
            logger.info(f"No match for {self.name} in {words_in_box}")
            return ""
        return result


@dataclasses.dataclass
class TableExtractor:
    top_label: str
    top_delta: float
    bottom_label: str
    bottom_delta: float
    row_margin: float
    index_col_left_label: str
    index_col_left_delta: float
    index_col_right_label: str
    index_col_right_delta: float
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
        
    def get_index_col_span(self):
        return (
            get_coordinate(
                self.tablemap, self.index_col_left_label,
                "Left", "Left_Default"
            ),
            get_coordinate(
                self.tablemap, self.index_col_right_label,
                "Left", "Left_Default"
            ),
        )
    
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
        page_words = words.loc[
            (words["Page"] == page)
        ]
        row_tops = page_words.loc[
            page_words["Left"].between(
                *self.get_index_col_span()
            )
            & page_words["Top"].between(
                self.table_top, self.table_bottom
            ),
            "Top"
        ].sort_values().reset_index(drop=True)
        row_bottoms =  row_tops - self.row_margin
        row_intervals = pd.DataFrame(
            {
                "row_top": row_tops,
                "row_bottom": row_bottoms.iloc[1:].reset_index(drop=True),
            }
        )
        return row_intervals.fillna(self.table_bottom)
    
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
