"""
csv_service.py — CSV loading, schema inference, summary statistics.
"""

import json
import math
import pandas as pd
import numpy as np
from pathlib import Path


def _safe_val(v):
    """Convert numpy scalars to native Python types safe for JSON."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


def load_csv(file_path: str) -> pd.DataFrame:
    """Load a CSV file into a DataFrame, guessing dtypes."""
    return pd.read_csv(file_path, low_memory=False)


def infer_schema(df: pd.DataFrame) -> dict[str, str]:
    """Return {column_name: dtype_string} for every column."""
    return {col: str(df[col].dtype) for col in df.columns}


def compute_null_counts(df: pd.DataFrame) -> dict[str, int]:
    return {col: int(df[col].isna().sum()) for col in df.columns}


def compute_stats(df: pd.DataFrame) -> dict[str, dict]:
    """
    Compute per-column summary statistics.
    Numeric columns: min/max/mean/std/median/unique.
    Categorical columns: top values and counts.
    """
    stats: dict[str, dict] = {}
    for col in df.columns:
        col_stats: dict = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "unique_count": int(df[col].nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            desc = df[col].describe()
            col_stats.update({
                "min": _safe_val(desc.get("min")),
                "max": _safe_val(desc.get("max")),
                "mean": _safe_val(desc.get("mean")),
                "std": _safe_val(desc.get("std")),
                "median": _safe_val(df[col].median()),
            })
        else:
            # Top 5 category frequencies
            vc = df[col].value_counts(dropna=True).head(5)
            col_stats["top_values"] = {str(k): int(v) for k, v in vc.items()}
        stats[col] = col_stats
    return stats


def sample_rows(df: pd.DataFrame, n: int = 50) -> list[dict]:
    """Return first n rows as a list of dicts, safe for JSON."""
    sample = df.head(n).copy()
    # Replace NaN/inf with None
    sample = sample.where(pd.notna(sample), other=None)
    records = sample.to_dict(orient="records")
    # Ensure all values are JSON-serialisable
    return [
        {k: _safe_val(v) for k, v in row.items()}
        for row in records
    ]


def build_snapshot_data(df: pd.DataFrame) -> dict:
    """Build all snapshot fields from a DataFrame."""
    return {
        "row_count": len(df),
        "col_count": len(df.columns),
        "schema_json": json.dumps(infer_schema(df)),
        "stats_json": json.dumps(compute_stats(df)),
        "null_counts_json": json.dumps(compute_null_counts(df)),
        "sample_json": json.dumps(sample_rows(df)),
    }
