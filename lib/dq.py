from typing import Iterable, Tuple
from pyspark.sql import DataFrame, functions as F

def require_columns(df: DataFrame, cols: Iterable[str]):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

def dedupe_by_keys(df: DataFrame, keys: Iterable[str]) -> DataFrame:
    return df.dropDuplicates(list(keys))

def count_nulls(df: DataFrame, cols: Iterable[str]) -> dict:
    return {c: df.filter(F.col(c).isNull()).count() for c in cols}

def normalize_lower(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn(col, F.lower(F.col(col)))
