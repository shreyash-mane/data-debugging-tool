"""
Layer 1: Data Profiler
======================
Generates a statistical profile of each column before any cleaning.
This is read-only — it never modifies data.

Produces per-column metadata used by all downstream layers.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """
    Return a full profile of *df*.

    Shape
    -----
    {
        "row_count": int,
        "col_count": int,
        "columns": {
            "<col_name>": <ColumnProfile>,
            ...
        }
    }
    """
    return {
        "row_count": len(df),
        "col_count": len(df.columns),
        "columns": {col: _profile_column(df[col], col) for col in df.columns},
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _profile_column(series: pd.Series, col_name: str) -> dict[str, Any]:
    """Compute statistics for a single column."""
    total = len(series)
    null_count = int(series.isna().sum())

    # Raw type counts before any coercion
    type_counts = _count_python_types(series)

    profile: dict[str, Any] = {
        "col_name": col_name,
        "total_count": total,
        "null_count": null_count,
        "null_pct": round(null_count / total * 100, 2) if total else 0.0,
        "unique_count": int(series.nunique(dropna=True)),
        "dtype": str(series.dtype),
        "type_counts": type_counts,
        "is_mixed_type": len([t for t, c in type_counts.items() if c > 0]) > 1,
        "sample_values": _safe_sample(series, n=5),
    }

    # Numeric stats (best-effort — ignore errors)
    numeric_series = pd.to_numeric(series, errors="coerce")
    if numeric_series.notna().sum() > 0:
        profile["numeric_stats"] = {
            "min": _safe_scalar(numeric_series.min()),
            "max": _safe_scalar(numeric_series.max()),
            "mean": _safe_scalar(numeric_series.mean()),
            "median": _safe_scalar(numeric_series.median()),
            "std": _safe_scalar(numeric_series.std()),
            "negative_count": int((numeric_series < 0).sum()),
        }
    else:
        profile["numeric_stats"] = None

    # String stats
    str_series = series.dropna().astype(str)
    profile["string_stats"] = {
        "has_leading_trailing_spaces": bool(
            str_series.str.strip().ne(str_series).any()
        ),
        "has_mixed_case": _has_mixed_case(str_series),
        "max_length": int(str_series.str.len().max()) if len(str_series) else 0,
        "min_length": int(str_series.str.len().min()) if len(str_series) else 0,
    }

    # Currency / word-number hints
    profile["has_currency_symbols"] = bool(
        str_series.str.contains(r"[£$€¥₹]", regex=True).any()
    )
    profile["has_word_numbers"] = _has_word_numbers(series)

    return profile


def _count_python_types(series: pd.Series) -> dict[str, int]:
    counts: dict[str, int] = {"int": 0, "float": 0, "str": 0, "bool": 0, "other": 0}
    for val in series.dropna():
        if isinstance(val, bool):
            counts["bool"] += 1
        elif isinstance(val, int):
            counts["int"] += 1
        elif isinstance(val, float):
            counts["float"] += 1
        elif isinstance(val, str):
            counts["str"] += 1
        else:
            counts["other"] += 1
    return counts


def _has_mixed_case(str_series: pd.Series) -> bool:
    """True if the column contains both upper and lower case distinct values."""
    lower_vals = str_series.str.lower().unique()
    orig_vals = str_series.unique()
    return len(orig_vals) != len(lower_vals)


_WORD_NUMBER_PATTERN = re.compile(
    r"\b(zero|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|"
    r"eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|"
    r"eighty|ninety|hundred|thousand)\b",
    re.IGNORECASE,
)


def _has_word_numbers(series: pd.Series) -> bool:
    str_vals = series.dropna().astype(str)
    return any(_WORD_NUMBER_PATTERN.search(v) for v in str_vals)


def _safe_sample(series: pd.Series, n: int = 5) -> list[Any]:
    non_null = series.dropna()
    sample = non_null.head(n).tolist()
    # JSON-serialisable
    return [v if not isinstance(v, float) or v == v else None for v in sample]


def _safe_scalar(val: Any) -> Any:
    """Convert numpy scalars to Python natives; handle NaN → None."""
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if hasattr(val, "item"):
        return val.item()
    return val
