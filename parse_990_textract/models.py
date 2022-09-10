import dataclasses
import re

import pandas as pd

from .utils import (
    cluster_words, cluster_x, combine_row, columnize, find_crossing_right,
    get_best_match, get_coordinate, get_cluster_coords, get_regex,
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
            logger.error(f"No match for {self.name} in {words_in_box}")
            return ""
        return result


@dataclasses.dataclass
class TableExtractor:
    header_top_label: str
    top_label: str
    bottom_label: str
    tablemap: pd.DataFrame
    fields: list[str]
    field_labels: pd.Series
        
    def get_word_delta(self, words, page):
        word_delta = getattr(self, "_word_delta", None)
        if word_delta is not None:
            return word_delta
        self._word_delta = words.loc[
            words["Page"] == page,
            "Height"
        ].median()
        return self._word_delta

    def get_header_top(self, words, page):
        header_top = getattr(self, "_header_top", None)
        if header_top is not None:
            return header_top
        self._header_top = get_coordinate(
            self.tablemap, self.header_top_label,
            "Top", "Top_Default"
        ) - self.get_word_delta(words, page)
        return self._header_top

    def get_table_top(self, words, page):
        table_top = getattr(self, "_table_top", None)
        if table_top is not None:
            return table_top
        self._table_top = get_coordinate(
            self.tablemap, self.top_label,
            "Top", "Top_Default"
        ) + self.get_word_delta(words, page)
        return self._table_top
    
    def get_table_bottom(self, words, page):
        table_bottom = getattr(self, "_table_bottom", None)
        if table_bottom is not None:
            return table_bottom
        try:
            self._table_bottom = get_coordinate(
                self.tablemap, self.bottom_label,
                "Top", "Top_Default",
            ) - self.get_word_delta(words, page)
        except KeyError:
            self._table_bottom = 1 - self.get_word_delta(words, page)
        return self._table_bottom

    def get_table_left(self, words, page):
        table_left = getattr(self, "_table_left", None)
        if table_left is not None:
            return table_left
        table_words = self.get_table_words(words, page)
        self._table_left = table_words["Left"].min()
        return self._table_left

    def get_table_right(self, words, page):
        table_right = getattr(self, "_table_right", None)
        if table_right is not None:
            return table_right
        table_words = self.get_table_words(words, page)
        self._table_right = table_words["Right"].min()
        return self._table_right
        
    def get_table_words(self, words, page):
        table_words  = getattr(self, "_table_words", None)
        if table_words is not None:
            return table_words
        self._table_words = words.loc[
            (words["Page"] == page)
            # Use Midpoint_Y to ensure we don't include anything from header
            & (words["Midpoint_Y"] > self.get_table_top(words, page))
            & (words["Top"] < self.get_table_bottom(words, page))
        ]
        return self._table_words

    def get_header_words(self, words, page):
        header_words = getattr(self, "_header_words", None)
        if header_words is not None:
            return header_words
        self._header_words = words.loc[
            (words["Page"] == page)
            & words["Midpoint_Y"].between(
                self.get_header_top(words, page),
                self.get_table_top(words, page)
                - self.get_word_delta(words, page),
            )
        ]
        return self._header_words

    def get_col_spans(self, words, page):
        col_spans = getattr(self, "_col_spans", None)
        if col_spans is not None:
            return col_spans
        init_left = self.field_labels.map(
            lambda x: get_coordinate(self.tablemap, x, "Left", "Left_Default"),
        ).reset_index(drop=True)
        init_right = pd.concat(
            [
                init_left.iloc[1:],
                pd.Series([1]),
            ],
            ignore_index=True,
        )
        init_spans = init_left.combine(
            init_right, lambda x, y: (x, y), 
        )
        header_words = self.get_header_words(words, page)
        table_words = self.get_table_words(words, page)
        combined_words = pd.concat([header_words, table_words])
        crossing_right = init_right.map(
            lambda x: find_crossing_right(combined_words, x)
        )
        new_right = init_right.where(
            crossing_right.isna(),
            crossing_right,
        )
        init_left.iloc[1:] = new_right.iloc[:-1]
        self._col_spans = init_left.combine(
            new_right,
            lambda x, y: (x, y)
        )
        return self._col_spans

    def get_rows(self, words, page):
        table_words = self.get_table_words(words, page)
        if not table_words.shape[0]:
            return []
        y_tol = table_words["Height"].median()
        word_clusters = cluster_words(
            table_words,
            table_words["Height"].min(),
            "Midpoint_Y",
        )
        col_spans = self.get_col_spans(words, page)
        columnized = columnize(word_clusters[0], col_spans)
        columnized.index = self.fields
        last_cluster_coords = pd.DataFrame.from_records(
            columnized.map(get_cluster_coords)
        )
        rows = []
        current_row = [columnized]
        top_ws = (
            last_cluster_coords["Top"].min()
            - self.get_table_top(words, page)
        )
        if top_ws > y_tol:
            alignment = "BOTTOM"
        else:
            alignment = "UNKNOWN"
        for count, cluster in enumerate(word_clusters[1:]):
            columnized = columnize(cluster, col_spans)
            columnized.index = self.fields
            cluster_coords = pd.DataFrame.from_records(
                columnized.map(get_cluster_coords)
            )
            y_delta = (
                cluster_coords["Top"].min() 
                - last_cluster_coords["Bottom"].max()
            )
            if y_delta > y_tol:
                combined_row = combine_row(current_row)
                rows.append(combined_row)
                current_row = [columnized]
            else:
                nonempty = cluster_coords.dropna().index.to_series()
                last_nonempty = last_cluster_coords.dropna().index.to_series()
                delta_cols = (~nonempty.isin(last_nonempty)).any()
                if not delta_cols and (alignment == "UNKNOWN"):
                    alignment = "TOP"
                    current_row.append(columnized)
                elif delta_cols and (alignment == "UNKNOWN"):
                    alignment = "BOTTOM"
                    current_row.append(columnized)
                elif delta_cols and (alignment == "TOP"):
                    combined_row = combine_row(current_row)
                    rows.append(combined_row)
                    current_row = [columnized]
                else:
                    current_row.append(columnized)
            last_cluster_coords = cluster_coords
        combined_row = combine_row(current_row)
        rows.append(combined_row)
        return rows
    
    def extract_rows(self, words, page):
        rows = self.get_rows(words, page)
        non_empty_rows = [row for row in rows if row.any()]
        if non_empty_rows:
            return pd.DataFrame(non_empty_rows, columns=self.fields)
        return pd.NA
