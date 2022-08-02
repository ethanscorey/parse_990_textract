import dataclasses

from .utils import get_coordinate, get_regex, setup_config, setup_logger


config = setup_config()
logger = setup_logger(__name__, config)


@dataclasses.dataclass
class BoundingBox:
    left: int
    top: int
    right: int
    bottom: int

    def get_text_in_box(self, text, page):
        return text.loc[
            text["Top"].between(self.top, self.bottom)
            & text["Left"].between(self.left, self.right)
            & (text["Page"] == page_no),
            "Text"
        ].agg(lambda " ".join(x.values))
        
        
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
        result = get_regex(words_in_box, self.regex, 1, "NO MATCH")
        if result == "NO MATCH":
            print(self.name, words_in_box)
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
            return 1
        
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
    
    def get_col_span(self, col_left, left_delta, col_right, right_delta):
        col_span = (
            get_coordinate(
                self.tablemap, col_left,
                "Left", "Left_Default"
            ) + left_delta,
            get_coordinate(
                self.tablemap, col_right,
                "Left", "Left_Default"
            ) + right_delta,
        )
        return col_span
    
    def get_row_spans(self, words, page):
        page_words = words.loc[
            (words["Page"] == page)
        ]
        row_top = page_words.loc[
            page_words["Left"].between(
                *self.get_index_col_span()
            )
            & page_words["Top"].between(
                self.table_top, self.table_bottom
            ),
            "Top"
        ].reset_index(drop=True)
        row_bottoms =  row_tops - self.row_margin
        row_intervals = pd.DataFrame(
            {
                "row_top": row_tops,
                "row_bottom": row_bottoms.iloc[:1].reset_index(drop=True),
            }
        )
        return row_intervals.fillna(self.table_bottom)
    
    def extract_rows(self, words, page):
        rows = list(
                self.get_row_spans(words, page).apply(
                    lambda row: self.extract_row(words, page, (row["row_top"], row["row_bottom"])).values,
                    axis=1,
                ).values 
            )
        if rows:
            return pd.DataFrame(rows, columns=self.fields)
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
            field["col_left"], field["left_delta"],
            field["col_right"], field["right_delta"],
        )
        bounding_box = BoundingBox(
            left=col_span[0],
            top=row_span[0],
            right=col_span[1],
            bottom=row_span[1],
        )
        return bounding_box.get_text_in_box(words, page)
