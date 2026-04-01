"""
Layer 5: Validator
==================
Applies business-rule validation AFTER normalisation.
Values that break rules are set to None (null) so the repair layer
can impute them cleanly.

Rules enforced:
  - age: must be 0 ≤ age ≤ 120 (configurable)
  - dates: must parse to a valid calendar date
  - money: must be non-negative numeric
  - IDs: records duplicates but does not alter values here
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Configuration defaults (can be overridden via config param)
# ---------------------------------------------------------------------------

DEFAULT_RULES: dict[str, Any] = {
    "age_min": 0,
    "age_max": 120,
    "money_min": 0,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate(
    df: pd.DataFrame,
    schema: dict[str, Any],
    audit_log: list[dict],
    rules: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Return a copy of *df* with invalid values replaced by None.
    Violations are appended to *audit_log*.
    """
    effective_rules = {**DEFAULT_RULES, **(rules or {})}
    df = df.copy()
    per_col = schema.get("per_column", {})

    for col in df.columns:
        semantic = per_col.get(col, {}).get("semantic_type", "unknown")

        if semantic == "age" or col in schema.get("age_columns", []):
            df[col] = _validate_age(df[col], col, effective_rules, audit_log)

        if semantic == "date" or col in schema.get("date_columns", []):
            df[col] = _validate_date(df[col], col, audit_log)

        if semantic == "money" or col in schema.get("money_columns", []):
            df[col] = _validate_money(df[col], col, effective_rules, audit_log)

        # Coerce all numeric-intended columns to numeric
        if schema.get("numeric_columns") and col in schema["numeric_columns"]:
            df[col] = _coerce_numeric(df[col], col, audit_log)

    return df


# ---------------------------------------------------------------------------
# Per-rule implementations
# ---------------------------------------------------------------------------

def _validate_age(
    series: pd.Series,
    col: str,
    rules: dict,
    audit_log: list[dict],
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    result = numeric.copy()

    age_min = rules["age_min"]
    age_max = rules["age_max"]

    invalid_mask = numeric.notna() & ((numeric < age_min) | (numeric > age_max))

    for idx in numeric[invalid_mask].index:
        audit_log.append({
            "column": col,
            "row_index": int(idx),
            "action": "invalidate_age",
            "from": series[idx],
            "to": None,
            "reason": f"age {numeric[idx]} outside valid range [{age_min}, {age_max}]",
            "confidence": "high",
            "layer": "validation",
        })
        result[idx] = None

    return result


def _validate_date(
    series: pd.Series,
    col: str,
    audit_log: list[dict],
) -> pd.Series:
    result = series.copy()

    for idx, val in series.items():
        if pd.isna(val):
            continue
        try:
            pd.to_datetime(str(val), format="%Y-%m-%d")
        except Exception:
            audit_log.append({
                "column": col,
                "row_index": int(idx),
                "action": "invalidate_date",
                "from": val,
                "to": None,
                "reason": f"'{val}' is not a valid date",
                "confidence": "high",
                "layer": "validation",
            })
            result[idx] = None

    return result


def _validate_money(
    series: pd.Series,
    col: str,
    rules: dict,
    audit_log: list[dict],
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    result = numeric.copy()
    money_min = rules["money_min"]

    invalid_mask = numeric.notna() & (numeric < money_min)
    for idx in numeric[invalid_mask].index:
        audit_log.append({
            "column": col,
            "row_index": int(idx),
            "action": "invalidate_money",
            "from": series[idx],
            "to": None,
            "reason": f"money value {numeric[idx]} below minimum {money_min}",
            "confidence": "high",
            "layer": "validation",
        })
        result[idx] = None

    return result


def _coerce_numeric(
    series: pd.Series,
    col: str,
    audit_log: list[dict],
) -> pd.Series:
    """Force column to numeric; non-coercible values → None with audit entry."""
    original = series.copy()
    numeric = pd.to_numeric(series, errors="coerce")

    # Find values that were non-null before but are null after coercion
    newly_nulled = original.notna() & numeric.isna()
    for idx in original[newly_nulled].index:
        audit_log.append({
            "column": col,
            "row_index": int(idx),
            "action": "coerce_numeric_failed",
            "from": original[idx],
            "to": None,
            "reason": f"'{original[idx]}' cannot be coerced to numeric",
            "confidence": "high",
            "layer": "validation",
        })

    return numeric
