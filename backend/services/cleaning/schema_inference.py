"""
Layer 2: Schema & Semantic Inference
=====================================
Infers the *semantic* meaning of each column (age, salary, date, email, …)
using column name heuristics + value-pattern analysis from Layer 1 profiles.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Value-check helpers — defined BEFORE _SEMANTIC_RULES
# ---------------------------------------------------------------------------

def _is_email_column(series: pd.Series) -> bool:
    email_re = re.compile(r"^[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}$")
    str_vals = series.dropna().astype(str)
    if len(str_vals) == 0:
        return False
    matched = str_vals.str.match(email_re).sum()
    return matched / len(str_vals) > 0.5


def _looks_like_date(series: pd.Series) -> bool:
    str_vals = series.dropna().astype(str).head(20)
    if len(str_vals) == 0:
        return False
    parsed = pd.to_datetime(str_vals, errors="coerce")
    return parsed.notna().sum() / len(str_vals) > 0.5


# ---------------------------------------------------------------------------
# Semantic type registry (functions must exist before this list)
# ---------------------------------------------------------------------------

_SEMANTIC_RULES: list[tuple[str, list[str], Any]] = [
    ("id",      [r"\bid\b", r"_id$", r"^id_", r"identifier", r"uuid", r"guid"], None),
    ("date",    [r"date", r"_at$", r"^at_", r"timestamp", r"created", r"updated", r"dob", r"birth"], None),
    ("email",   [r"email", r"e.?mail"], _is_email_column),
    ("age",     [r"\bage\b", r"_age$", r"^age_", r"years.?old"], None),
    ("money",   [r"salary", r"wage", r"pay", r"income", r"revenue", r"amount",
                 r"price", r"cost", r"fee", r"budget", r"compensation"], None),
    ("country", [r"country", r"nation", r"nationality"], None),
    ("name",    [r"^name$", r"_name$", r"^name_", r"firstname", r"lastname",
                 r"first.?name", r"last.?name", r"full.?name", r"surname"], None),
    ("phone",   [r"phone", r"mobile", r"tel\b", r"telephone", r"cell"], None),
    ("gender",  [r"gender", r"sex\b"], None),
    ("boolean", [r"^is_", r"^has_", r"^was_", r"^did_", r"^can_"], None),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def infer_schema(df: pd.DataFrame, profile: dict[str, Any]) -> dict[str, Any]:
    per_column: dict[str, dict[str, str]] = {}
    buckets: dict[str, list[str]] = {
        "numeric_columns": [],
        "date_columns": [],
        "categorical_columns": [],
        "id_columns": [],
        "money_columns": [],
        "age_columns": [],
        "email_columns": [],
        "name_columns": [],
        "boolean_columns": [],
    }

    for col in df.columns:
        col_profile = profile["columns"].get(col, {})
        semantic_type, confidence = _infer_column(col, df[col], col_profile)
        storage_type = _storage_type(df[col], col_profile)
        per_column[col] = {"semantic_type": semantic_type, "storage_type": storage_type, "confidence": confidence}
        _assign_bucket(col, semantic_type, storage_type, buckets)

    return {"per_column": per_column, **buckets}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _infer_column(col_name: str, series: pd.Series, col_profile: dict[str, Any]) -> tuple[str, str]:
    name_lower = col_name.lower().replace(" ", "_")

    for semantic_type, name_patterns, value_check_fn in _SEMANTIC_RULES:
        name_matched = any(re.search(p, name_lower) for p in name_patterns)
        if name_matched:
            if value_check_fn is not None and value_check_fn(series):
                return semantic_type, "high"
            if value_check_fn is not None:
                return semantic_type, "medium"
            return semantic_type, "high"
        if value_check_fn is not None and value_check_fn(series):
            return semantic_type, "medium"

    if _looks_like_date(series):
        return "date", "medium"
    if col_profile.get("has_currency_symbols"):
        return "money", "medium"
    return "unknown", "low"


def _storage_type(series: pd.Series, col_profile: dict[str, Any]) -> str:
    dtype = str(series.dtype)
    if "int" in dtype or "float" in dtype:
        return "numeric"
    if "bool" in dtype:
        return "boolean"
    if "datetime" in dtype:
        return "date"
    numeric_series = pd.to_numeric(series, errors="coerce")
    non_null_total = series.notna().sum()
    if non_null_total > 0 and numeric_series.notna().sum() / non_null_total > 0.7:
        return "numeric"
    if _looks_like_date(series):
        return "date"
    return "text"


def _assign_bucket(col: str, semantic_type: str, storage_type: str, buckets: dict[str, list[str]]) -> None:
    mapping = {
        "id": "id_columns", "date": "date_columns", "email": "email_columns",
        "age": "age_columns", "money": "money_columns", "name": "name_columns",
        "boolean": "boolean_columns",
    }
    if semantic_type in mapping:
        buckets[mapping[semantic_type]].append(col)
    elif storage_type == "numeric":
        buckets["numeric_columns"].append(col)
    else:
        buckets["categorical_columns"].append(col)
