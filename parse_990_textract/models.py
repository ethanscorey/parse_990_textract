import dataclasses
import re

import pandas as pd

from .utils import (
    cluster_words, cluster_x, columnize, get_best_match, get_coordinate,
    get_cluster_coords, get_regex, setup_config, setup_logger
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
        
    def get_word_delta(self, words, page):
        word_delta = getattr(self, "_word_delta", None)
        if word_delta is not None:
            return word_delta
        self._word_delta = words.loc[
            words["Page"] == page,
            "Height"
        ].min()
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
            ) + self.get_word_delta(words, page)
        except KeyError:
            self._table_bottom = 1 - self.get_word_delta(words, page)
        return self._table_bottom
        
    def get_table_words(self, words, page):
        table_words = words.loc[
            (words["Page"] == page)
            & words["Midpoint_Y"].between(
                self.get_table_top(words, page),
                self.get_table_bottom(words, page)
            )
        ]
        return table_words

    def get_header_words(self, words, page):
        header_words = words.loc[
            (words["Page"] == page)
            & words["Midpoint_Y"].between(
                self.get_header_top(words, page),
                self.get_table_top(words, page)
                - self.get_word_delta(words, page),
            )
        ]
        return header_words

    def get_col_spans(self, words, page):
        header_words = self.get_header_words(words, page)
        tolerance = header_words["Width"].min()
        x_clusters = cluster_x(header_words, tolerance)
        while len(x_clusters) != self.fields.count():
            if len(x_clusters) < self.fields.count():
                tolerance *= .95
            else:
                tolerance *= 1.05
            x_clusters = cluster_x(header_words, tolerance)
            
        midpoints = pd.Series(
            cluster["Midpoint_X"].median() for cluster in x_clusters
        )
        left_bounds = pd.Series(
            cluster["Left"].min() for cluster in x_clusters[1:]
        )
        right_bounds = pd.Series(
            [cluster["Right"].max() for cluster in x_clusters[:-1]]
        )
        offsets = (right_bounds - left_bounds) / 2
        full_left = pd.concat(
            [pd.Series([0]), left_bounds + offsets]
        ).reset_index(drop=True)
        full_right = pd.concat(
            [right_bounds - offsets, pd.Series([1])]
        ).reset_index(drop=True)
        return full_left.combine(full_right, lambda x, y: (x, y))

    def get_rows(self, words, page):
        table_words = self.get_table_words(words, page)
        y_tol = table_words["Height"].max() * 1.5
        x_tol = table_words["Width"].median()
        word_clusters = cluster_words(
            table_words,
            table_words["Height"].min(),
            "Midpoint_Y",
        )
        col_spans = self.get_col_spans(words, page)
        columnized = columnize(word_clusters[0], col_spans)
        columnized.index = self.fields
        last_col_coords = pd.DataFrame.from_records(
            columnized.map(get_cluster_coords)
        )
        sum_y_delta = last_col_coords["Height"].max()
        rows = []
        current_row = [columnized]
        
        for count, cluster in enumerate(word_clusters[1:]):
            columnized = columnize(cluster, col_spans)
            columnized.index = self.fields
            col_coords = pd.DataFrame.from_records(
                columnized.map(get_cluster_coords)
            )
            nonempty = col_coords.dropna().index.to_series()
            last_nonempty = last_col_coords.dropna().index.to_series()
            delta_cols = (
                (nonempty.count() > last_nonempty.count())
                or (~nonempty.isin(last_nonempty)).any()
            )
            y_delta = (
                col_coords["Midpoint_Y"].median() 
                - last_col_coords["Midpoint_Y"].median()
            )
            sum_y_delta += y_delta
            mean_y_delta = sum_y_delta / (count + 1)
            min_y_delta = mean_y_delta * 0.5
            if (delta_cols or (y_delta > y_tol)) and (y_delta > min_y_delta):
                combined_row = pd.Series([
                    line.map(
                        lambda x: x.sort_values(
                            by="Left"
                        ).reset_index(drop=True)["Text"].fillna("")
                    ).agg(
                        lambda x: " ".join(x.values)
                    ) + " "
                    for line in current_row
                ]).sum().str.strip()
                rows.append(combined_row)
                current_row = [columnized]
            else:
                current_row.append(columnized)
            last_col_coords = col_coords
        combined_row = pd.Series([
            line.map(
                lambda x: x.sort_values(
                    by="Left"
                ).reset_index(drop=True)["Text"].fillna("")
            ).agg(
                lambda x: " ".join(x.values)
            ) + " "
            for line in current_row
        ]).sum().str.strip()
        rows.append(combined_row)
        return rows
    
    def extract_rows(self, words, page):
        rows = self.get_rows(words, page)
        non_empty_rows = [row for row in rows if row.any()]
        if non_empty_rows:
            return pd.DataFrame(non_empty_rows, columns=self.fields)
        return pd.NA
