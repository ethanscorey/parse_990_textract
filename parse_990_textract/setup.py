import re

import pandas as pd


def load_extractor_df(fname):
    """Read extractors from CSV and compile regexes."""
    return pd.read_csv(fname).assign(regex=lambda df: df["regex"].map(re.compile))
