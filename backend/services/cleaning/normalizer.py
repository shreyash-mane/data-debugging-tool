"""
Layer 4: Normalizer
===================
Applies safe, reversible transformations to standardise value formats.
Only transforms values — does NOT impute or drop rows.

Transformations applied:
  - strip whitespace
  - normalise casing (categorical / name columns)
  - convert word-numbers to integers ("thirty" → 30)
  - strip currency symbols and commas ("£45,000" → 45000.0)
  - standardise date strings to YYYY-MM-DD
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .issue_detector import _WORD_NUMBER_MAP, _WORD_NUMBER_RE, _DATE_FORMATS

# Currency regexes (defined here so _strip_currency can use them)
_CURRENCY_RE = re.compile(r"[£$€¥₹]")
_CURRENCY_STRIP_RE = re.compile(r"[£$€¥₹,\s]")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize(
    df: pd.DataFrame,
    schema: dict[str, Any],
    audit_log: list[dict],
) -> pd.DataFrame:
    """Return a copy of *df* with normalisation applied."""
    df = df.copy()

    per_col = schema.get("per_column", {})

    for col in df.columns:
        col_schema = per_col.get(col, {})
        semantic = col_schema.get("semantic_type", "unknown")
        storage = col_schema.get("storage_type", "text")

        df[col] = _strip_whitespace(df[col], col, audit_log)

        if storage == "text" or semantic in ("country", "gender", "unknown", "categorical", "name"):
            df[col] = _normalise_case(df[col], col, semantic, audit_log)

        if col in schema.get("money_columns", []) or col in schema.get("age_columns", []):
            df[col] = _strip_currency(df[col], col, audit_log)

        df[col] = _convert_word_numbers(df[col], col, audit_log)

        if semantic == "date" or col in schema.get("date_columns", []):
            df[col] = _normalise_dates(df[col], col, audit_log)

    return df


# ---------------------------------------------------------------------------
# Individual normalisation steps
# ---------------------------------------------------------------------------

def _strip_whitespace(
    series: pd.Series, col: str, audit_log: list[dict]
) -> pd.Series:
    result = series.copy()
    for idx, val in series.items():
        if not isinstance(val, str):
            continue
        stripped = val.strip()
        if stripped != val:
            result[idx] = stripped
            audit_log.append({
                "column": col,
                "row_index": int(idx),
                "action": "strip_whitespace",
                "from": repr(val),
                "to": repr(stripped),
                "confidence": "high",
                "layer": "normalization",
            })
    return result


def _normalise_case(
    series: pd.Series,
    col: str,
    semantic: str,
    audit_log: list[dict],
) -> pd.Series:
    result = series.copy()
    for idx, val in series.items():
        if not isinstance(val, str):
            continue
        new_val = val.title() if semantic in ("name", "country") else val.lower()
        if new_val != val:
            result[idx] = new_val
            audit_log.append({
                "column": col,
                "row_index": int(idx),
                "action": "normalise_case",
                "from": repr(val),
                "to": repr(new_val),
                "confidence": "medium",
                "layer": "normalization",
            })
    return result


def _strip_currency(
    series: pd.Series, col: str, audit_log: list[dict]
) -> pd.Series:
    # Cast to object so we can mix strings and floats during the transition
    result = series.astype(object).copy()
    for idx, val in series.items():
        if pd.isna(val):
            continue
        val_str = str(val)
        if not _CURRENCY_RE.search(val_str):
            continue
        cleaned = _CURRENCY_STRIP_RE.sub("", val_str)
        try:
            numeric_val = float(cleaned)
            result[idx] = numeric_val
            audit_log.append({
                "column": col,
                "row_index": int(idx),
                "action": "strip_currency",
                "from": val,
                "to": numeric_val,
                "confidence": "high",
                "layer": "normalization",
            })
        except ValueError:
            pass  # Validator will catch unconvertable values
    return result


def _word_number_to_int(text: str) -> int | None:
    """
    Convert simple word-number strings to integers.
    Handles single words and basic combinations like "twenty five" → 25.
    Returns None if conversion is ambiguous or fails.
    """
    text = text.strip().lower()
    words = text.split()
    total = 0
    current = 0

    for word in words:
        val = _WORD_NUMBER_MAP.get(word)
        if val is None:
            return None  # Unknown word in the string
        if val == 100:
            current = (current or 1) * 100
        elif val == 1000:
            total += (current or 1) * 1000
            current = 0
        else:
            current += val

    return total + current if (total + current) > 0 else None


def _convert_word_numbers(
    series: pd.Series, col: str, audit_log: list[dict]
) -> pd.Series:
    str_mask = series.notna() & series.apply(lambda v: isinstance(v, str))
    if not str_mask.any():
        return series

    str_vals = series[str_mask]
    word_num_mask = str_vals.astype(str).apply(
        lambda v: bool(_WORD_NUMBER_RE.search(v))
    )
    if not word_num_mask.any():
        return series

    result = series.copy()
    for idx in str_vals[word_num_mask].index:
        original = series[idx]
        converted = _word_number_to_int(str(original))
        if converted is not None:
            result[idx] = converted
            audit_log.append({
                "column": col,
                "row_index": int(idx),
                "action": "convert_word_number",
                "from": original,
                "to": converted,
                "confidence": "high",
                "layer": "normalization",
            })

    return result


def _normalise_dates(
    series: pd.Series, col: str, audit_log: list[dict]
) -> pd.Series:
    """Parse dates from any known format and return ISO 8601 strings."""
    result = series.copy()

    for idx, val in series.items():
        if pd.isna(val):
            continue
        val_str = str(val).strip()

        # Already looks like YYYY-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", val_str):
            # Still validate it
            try:
                parsed = pd.to_datetime(val_str, format="%Y-%m-%d")
                result[idx] = parsed.strftime("%Y-%m-%d")
            except Exception:
                result[idx] = None  # Validator will flag
            continue

        # Try each known format
        converted = False
        for fmt in _DATE_FORMATS:
            try:
                parsed = pd.to_datetime(val_str, format=fmt)
                new_val = parsed.strftime("%Y-%m-%d")
                if new_val != val_str:
                    audit_log.append({
                        "column": col,
                        "row_index": int(idx),
                        "action": "normalise_date",
                        "from": val_str,
                        "to": new_val,
                        "confidence": "high",
                        "layer": "normalization",
                    })
                result[idx] = new_val
                converted = True
                break
            except Exception:
                continue

        if not converted:
            # Leave as-is; validator + repair will handle
            pass

    return result
