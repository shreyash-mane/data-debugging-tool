"""
Layer 6: Repair / Imputation
=============================
Fills remaining null values after normalisation + validation.

Strategy selection:
  - numeric columns      → skewness-based (|skew| < 0.5 → mean, else → median)
  - categorical columns  → mode
  - date columns         → no imputation (flag only)
  - id columns           → no imputation (flag only)

Confidence:
  - null_pct < 5%  → high   (safe to auto-fill)
  - null_pct 5–20% → medium (suggest to user)
  - null_pct > 20% → low    (flag for review, but still fill)
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def repair(
    df: pd.DataFrame,
    schema: dict[str, Any],
    profile: dict[str, Any],  # pre-clean profile for null_pct reference
    audit_log: list[dict],
) -> pd.DataFrame:
    """Return a copy of *df* with null values imputed where appropriate."""
    df = df.copy()
    per_col = schema.get("per_column", {})

    for col in df.columns:
        if df[col].isna().sum() == 0:
            continue

        semantic = per_col.get(col, {}).get("semantic_type", "unknown")
        null_pct = profile["columns"][col]["null_pct"]
        confidence = _confidence_from_null_pct(null_pct)

        # Skip imputation for IDs and dates
        if semantic in ("id", "date"):
            _log_skipped(col, semantic, audit_log)
            continue

        # Numeric columns → skewness-based fill (mean if symmetric, median if skewed)
        if (
            col in schema.get("numeric_columns", [])
            or col in schema.get("age_columns", [])
            or col in schema.get("money_columns", [])
        ):
            df[col] = _fill_numeric(df[col], col, confidence, audit_log)
            continue

        # Categorical / text → mode
        df[col] = _fill_categorical(df[col], col, confidence, audit_log)

    return df


# ---------------------------------------------------------------------------
# Fill strategies
# ---------------------------------------------------------------------------

def _fill_numeric(
    series: pd.Series,
    col: str,
    confidence: str,
    audit_log: list[dict],
) -> pd.Series:
    """
    Skewness-based numeric imputation.
    |skew| < 0.5  → use mean  (distribution is symmetric enough)
    |skew| >= 0.5 → use median (distribution is skewed — robust to outliers)

    Falls back to median if skewness cannot be computed (< 3 non-null values).
    """
    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()

    if len(non_null) == 0:
        return series  # All null — nothing to fill with

    # Decide fill strategy based on skewness
    if len(non_null) >= 3:
        skewness = float(non_null.skew())
        if abs(skewness) < 0.5:
            fill_val = float(non_null.mean())
            strategy = "impute_mean"
        else:
            fill_val = float(non_null.median())
            strategy = "impute_median"
    else:
        fill_val = float(non_null.median())
        strategy = "impute_median"

    if pd.isna(fill_val):
        return series

    null_idx = numeric[numeric.isna()].index
    result = numeric.copy()
    result[null_idx] = fill_val

    for idx in null_idx:
        audit_log.append({
            "column": col,
            "row_index": int(idx),
            "action": strategy,
            "from": None,
            "to": round(fill_val, 4),
            "confidence": confidence,
            "layer": "repair",
        })

    return result


def _fill_categorical(
    series: pd.Series,
    col: str,
    confidence: str,
    audit_log: list[dict],
) -> pd.Series:
    mode_result = series.mode(dropna=True)
    if mode_result.empty:
        return series

    fill_val = mode_result.iloc[0]
    null_idx = series[series.isna()].index
    result = series.copy()
    result[null_idx] = fill_val

    for idx in null_idx:
        audit_log.append({
            "column": col,
            "row_index": int(idx),
            "action": "impute_mode",
            "from": None,
            "to": fill_val,
            "confidence": confidence,
            "layer": "repair",
        })

    return result


def _confidence_from_null_pct(null_pct: float) -> str:
    if null_pct < 5:
        return "high"
    if null_pct <= 20:
        return "medium"
    return "low"


def _log_skipped(col: str, semantic: str, audit_log: list[dict]) -> None:
    audit_log.append({
        "column": col,
        "row_index": None,
        "action": "skip_imputation",
        "from": None,
        "to": None,
        "reason": f"Imputation skipped for semantic type '{semantic}'",
        "confidence": "high",
        "layer": "repair",
    })
