import pandas as pd
import numpy as np
import re
from typing import List, Dict, Optional
from datetime import datetime

# --------------------------------------------------
# Missing Value Handling
# --------------------------------------------------

def standardize_nulls(
    df: pd.DataFrame,
    null_values: List = None
) -> pd.DataFrame:
    """
    Replace common null representations with np.nan.
    """
    if null_values is None:
        null_values = [
            "", " ", "NULL", "null",
            "N/A", "n/a", "NA", "na",
            "None", "none", "-", "--"
        ]

    return df.replace(null_values, np.nan)


def fill_missing(
    df: pd.DataFrame,
    strategy: Dict[str, str]
) -> pd.DataFrame:
    """
    Fill missing values using strategy:
    {'age':'median', 'city':'mode', 'salary':'mean'}
    """
    df = df.copy()

    for col, method in strategy.items():
        if method == "mean":
            df[col] = df[col].fillna(df[col].mean())

        elif method == "median":
            df[col] = df[col].fillna(df[col].median())

        elif method == "mode":
            df[col] = df[col].fillna(df[col].mode()[0])

        elif method == "zero":
            df[col] = df[col].fillna(0)

    return df


# --------------------------------------------------
# Duplicate Removal
# --------------------------------------------------

def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Remove duplicate records.
    """
    return df.drop_duplicates(subset=subset)


# --------------------------------------------------
# String Cleaning
# --------------------------------------------------

def clean_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace from all string columns.
    """
    df = df.copy()

    for col in df.select_dtypes(include="object"):
        df[col] = df[col].astype(str).str.strip()

    return df


def normalize_text(
    df: pd.DataFrame,
    columns: List[str],
    case: str = "lower"
) -> pd.DataFrame:
    """
    Normalize text columns.
    """
    df = df.copy()

    for col in columns:
        if case == "lower":
            df[col] = df[col].str.lower()

        elif case == "upper":
            df[col] = df[col].str.upper()

        elif case == "title":
            df[col] = df[col].str.title()

    return df


def remove_special_characters(
    df: pd.DataFrame,
    columns: List[str]
) -> pd.DataFrame:
    """
    Remove special characters.
    """
    df = df.copy()

    for col in columns:
        df[col] = df[col].str.replace(
            r'[^a-zA-Z0-9\s]',
            '',
            regex=True
        )

    return df


# --------------------------------------------------
# Data Type Conversion
# --------------------------------------------------

def convert_numeric(
    df: pd.DataFrame,
    columns: List[str]
) -> pd.DataFrame:
    """
    Convert columns to numeric.
    """
    df = df.copy()

    for col in columns:
        df[col] = pd.to_numeric(
            df[col],
            errors='coerce'
        )

    return df


def convert_datetime(
    df: pd.DataFrame,
    columns: List[str]
) -> pd.DataFrame:
    """
    Convert columns to datetime.
    """
    df = df.copy()

    for col in columns:
        df[col] = pd.to_datetime(
            df[col],
            errors='coerce'
        )

    return df

def milliseconds_to_date(timestamp_ms: int) -> datetime:
    """
    Convert a Unix timestamp in milliseconds to a datetime object.
    """
    return datetime.fromtimestamp(timestamp_ms // 1000)


# --------------------------------------------------
# Outlier Handling
# --------------------------------------------------

def remove_outliers_iqr(
    df: pd.DataFrame,
    column: str,
    multiplier: float = 1.5
) -> pd.DataFrame:
    """
    Remove outliers using IQR.
    """
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)

    iqr = q3 - q1

    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr

    return df[
        (df[column] >= lower) &
        (df[column] <= upper)
    ]


# --------------------------------------------------
# Column Standardization
# --------------------------------------------------

def standardize_column_names(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Convert column names to snake_case.
    """
    df = df.copy()

    df.columns = [
        re.sub(
            r'_+',
            '_',
            re.sub(
                r'[^a-z0-9]',
                '_',
                col.lower().strip()
            )
        ).strip('_')
        for col in df.columns
    ]

    return df


# --------------------------------------------------
# Validation
# --------------------------------------------------

def validate_required_columns(
    df: pd.DataFrame,
    required: List[str]
):
    """
    Validate required columns exist.
    """
    missing = set(required) - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing columns: {missing}"
        )


def validate_null_percentage(
    df: pd.DataFrame,
    threshold: float = 0.5
):
    """
    Raise error if null percentage exceeds threshold.
    """
    null_pct = df.isnull().mean()

    failed = null_pct[null_pct > threshold]

    if len(failed):
        raise ValueError(
            f"High null percentage: {failed.to_dict()}"
        )

def remove_parentesis(
    df: pd.DataFrame,
    columns: List[str]
) -> pd.DataFrame:
    """
    Remove special characters.
    """
    df = df.copy()

    for col in columns:
        df[col] = df[col].str.replace(
            r"[\[\]\(\)]",
            '',
            regex=True
        )

    return df




# --------------------------------------------------
# End-to-End Clean Pipeline
# --------------------------------------------------

def basic_data_cleanse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standard cleansing pipeline.
    """
    df = standardize_column_names(df)
    df = standardize_nulls(df)
    df = clean_whitespace(df)
    df = remove_duplicates(df)

    return df