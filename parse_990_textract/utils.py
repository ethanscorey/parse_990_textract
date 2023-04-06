import logging
import math
import os
import re

import pandas as pd
from dotenv import dotenv_values


def setup_config(env_file_var="ENVFILE", env_file_default=".env.local"):
    return dotenv_values(os.getenv(env_file_var, env_file_default))


def setup_logger(
    logger_name,
    config,
    log_level_var="PARSE_990_TEXTRACT_OUTPUT_LOG_LEVEL",
    log_level_default="DEBUG",
):
    logger = logging.getLogger(logger_name)
    logger.setLevel(
        getattr(logging, config.get(log_level_var, log_level_default))
    )
    return logger


config = setup_config()
logger = setup_logger(__name__, config)


def trunc_num(value, places):
    return math.trunc(value * 10**places) / 10**places


def get_regex(string, regex, match_group=0, alt_value=None):
    match = regex.search(string)
    if match is not None:
        return match.group(match_group)
    return alt_value


def get_best_match(string, regex, alt_value=None):
    match = regex.search(string)
    if match is not None:
        return max(
            match.groups(), key=lambda x: len(x) if x is not None else 0
        )
    return alt_value


def get_coordinate(roadmap, index, col, default):
    value = roadmap.at[index, col]
    if pd.isna(value):
        value = roadmap.loc[index, default]
    return value


def clean_num(text) -> str:
    if re.search(r"^[\doOliIZS ,$.()-]+$", str(text)):
        fix_zeroes = re.sub(r"[oO]", "0", str(text).strip())
        fix_ones = re.sub(r"[liI]", "1", fix_zeroes)
        fix_twos = re.sub("Z", "2", fix_ones)
        fix_fives = re.sub(r"S", "5", fix_twos)
        cleaned = re.sub(r"[^-.\d()]|\.$|(?<=.)[-(]|\)(?=.)", "", fix_fives)
        if cleaned.startswith("(") and cleaned.endswith(")"):
            return f"-{cleaned[1:-1]}"
        else:
            return re.sub(r"[()]", "", cleaned)
    if text:
        logger.info(str(text))
    return ""


def cluster_words(words, tolerance, attribute):
    if (tolerance == 0) or (words.shape[0] < 2):
        return [
            pd.DataFrame([word])
            for (_idx, word) in words.sort_values(by=attribute).iterrows()
        ]
    groups = []
    sorted_words = words.sort_values(by=attribute)
    current_group = [sorted_words.iloc[0]]
    last = sorted_words.iloc[0][attribute]
    for _idx, word in sorted_words.iloc[1:].iterrows():
        if word[attribute] <= (last + tolerance):
            current_group.append(word)
        else:
            groups.append(current_group)
            current_group = [word]
        last = word[attribute]
    groups.append(current_group)
    return [pd.DataFrame(group) for group in groups]


def get_cluster_coords(cluster):
    cluster_coords = {
        "Left": cluster["Left"].min(),
        "Right": cluster["Right"].max(),
        "Height": cluster["Height"].max(),
        "Midpoint_X": cluster["Midpoint_X"].median(),
        "Midpoint_Y": cluster["Midpoint_Y"].median(),
        "Top": cluster["Top"].min(),
        "Bottom": cluster["Bottom"].min(),
    }
    cluster_coords["Width"] = cluster_coords["Right"] - cluster_coords["Left"]
    return cluster_coords


def columnize(word_cluster, col_spans):
    return col_spans.map(
        lambda span: word_cluster.loc[
            word_cluster["Right"].between(*span, inclusive="right")
        ]
    )


def cluster_x(words, tolerance):
    if (tolerance == 0) or (words.shape[0] < 2):
        return [
            [word] for (_idx, word) in words.sort_values(by="Left").iterrows()
        ]
    groups = []
    sorted_words = words.sort_values(by="Left")
    current_group = [sorted_words.iloc[0]]
    last = sorted_words.iloc[0]["Right"]
    for _idx, word in sorted_words.iloc[1:].iterrows():
        if word["Left"] <= (last + tolerance):
            current_group.append(word)
        else:
            groups.append(current_group)
            current_group = [word]
        last = max((word["Right"], last))
    groups.append(current_group)
    return [pd.DataFrame(group) for group in groups]


def rotate(textract_obj):
    height = textract_obj["Height"]
    width = textract_obj["Width"]
    left = textract_obj["Left"]
    right = textract_obj["Right"]
    top = textract_obj["Top"]
    bottom = textract_obj["Bottom"]
    midpoint_x = textract_obj["Midpoint_X"]
    midpoint_y = textract_obj["Midpoint_Y"]
    new_obj = textract_obj.copy()
    new_obj["Height"] = width
    new_obj["Width"] = height
    new_obj["Left"] = 1 - bottom
    new_obj["Right"] = 1 - top
    new_obj["Top"] = left
    new_obj["Bottom"] = right
    new_obj["Midpoint_X"] = 1 - midpoint_y
    new_obj["Midpoint_Y"] = midpoint_x
    return new_obj


def id_rotated_pages(df):
    lines = df.loc[df["BlockType"] == "LINE"]
    by_page = lines.groupby("Page")
    return (by_page["Width"].mean() / by_page["Height"].mean())[
        lambda x: x < 0.5
    ].index.values


def rotate_pages(df):
    rotated_pages = id_rotated_pages(df.loc[df["BlockType"] == "LINE"])
    return df.mask(df["Page"].isin(rotated_pages), rotate, axis=1)


def combine_row(row):
    combined_row = (
        pd.Series(
            [
                line.map(
                    lambda x: x.sort_values(by="Left")
                    .reset_index(drop=True)["Text"]
                    .fillna("")
                ).agg(lambda x: " ".join(x.values))
                + " "
                for line in row
            ]
        )
        .sum()
        .str.strip()
    )
    return combined_row


def find_crossing_right(df, right):
    return df.loc[
        (df["Right"] > right * 1.01) & (df["Left"] < right), "Left"
    ].min()


def sort_words(df):
    """Cluster words into lines, then sort left to right."""
    lines = cluster_words(df, df["Height"].min(), "Midpoint_Y")
    sorted_words = pd.concat(
        [
            cluster.sort_values("Left") for cluster in lines
        ]
    )
    word_order = sorted_words.reset_index().index
    return pd.Series(word_order, index=sorted_words.index)
