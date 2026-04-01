"""
Layer 3: Issue Detector
=======================
Scans the dataframe using profile + schema info and produces a flat list of
Issue objects.  Detection only — no data is modified here.

Each issue has:
  column, issue_type, detail, examples, severity, recommended_action,
  layer, confidence
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Issue:
    column: str
    issue_type: str
    detail: str
    examples: list[Any]
    severity: str           # "critical" | "warning" | "info"
    recommended_action: str
    layer: str = "issue_detection"
    confidence: str = "high"  # "high" | "medium" | "low"

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "issue_type": self.issue_type,
            "detail": self.detail,
            "examples": self.examples,
            "severity": self.severity,
            "recommended_action": self.recommended_action,
            "layer": self.layer,
            "confidence": self.confidence,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_issues(
    df: pd.DataFrame,
    profile: dict[str, Any],
    schema: dict[str, Any],
) -> list[Issue]:
    """Run all detectors and return a merged list of Issue objects."""
    issues: list[Issue] = []

    # Global (cross-column) checks
    issues.extend(_detect_empty_rows(df))
    issues.extend(_detect_duplicate_rows(df))
    issues.extend(_detect_duplicate_ids(df, schema))

    # Per-column checks
    for col in df.columns:
        col_profile = profile["columns"][col]
        col_schema = schema["per_column"].get(col, {})
        semantic = col_schema.get("semantic_type", "unknown")
        series = df[col]

        issues.extend(_detect_missing_values(col, series, col_profile))
        issues.extend(_detect_mixed_types(col, series, col_profile))
        issues.extend(_detect_whitespace(col, series, col_profile))
        issues.extend(_detect_mixed_case(col, series, col_profile, semantic))
        issues.extend(_detect_word_numbers(col, series, col_profile))
        issues.extend(_detect_currency_strings(col, series, col_profile, semantic))
        issues.extend(_detect_negative_ages(col, series, semantic))
        issues.extend(_detect_impossible_ages(col, series, semantic))
        issues.extend(_detect_invalid_dates(col, series, semantic))
        issues.extend(_detect_mixed_date_formats(col, series, semantic))

    return issues


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------

def _detect_missing_values(col: str, series: pd.Series, profile: dict) -> list[Issue]:
    n = profile["null_count"]
    if n == 0:
        return []
    pct = profile["null_pct"]
    severity = "critical" if pct > 30 else "warning"
    return [Issue(
        column=col,
        issue_type="missing_values",
        detail=f"{n} missing values ({pct}%)",
        examples=[],
        severity=severity,
        recommended_action="impute with median (numeric) or mode (categorical)",
        confidence="high",
    )]


def _detect_empty_rows(df: pd.DataFrame) -> list[Issue]:
    """Detect rows where all data columns (non-ID) are null or empty string."""
    check = df.replace(r"^\s*$", pd.NA, regex=True)
    id_like = [c for c in df.columns if re.search(r"\bid\b|_id$|^id_", c.lower())]
    data_cols = [c for c in df.columns if c not in id_like]
    col_set = data_cols if data_cols else list(df.columns)

    all_null_mask = check[col_set].isnull().all(axis=1)
    n = int(all_null_mask.sum())
    if n == 0:
        return []
    empty_indices = df[all_null_mask].index.tolist()
    return [Issue(
        column="__all__",
        issue_type="empty_rows",
        detail=f"{n} empty row(s) — all data fields are null (row indices: {empty_indices})",
        examples=empty_indices,
        severity="critical",
        recommended_action="drop empty rows — they carry no information",
        confidence="high",
    )]


def _detect_duplicate_rows(df: pd.DataFrame) -> list[Issue]:
    n = int(df.duplicated().sum())
    if n == 0:
        return []
    return [Issue(
        column="__all__",
        issue_type="duplicate_rows",
        detail=f"{n} fully duplicate rows detected",
        examples=[],
        severity="warning",
        recommended_action="drop duplicate rows (keep first)",
        confidence="high",
    )]


def _detect_duplicate_ids(df: pd.DataFrame, schema: dict) -> list[Issue]:
    issues = []
    for col in schema.get("id_columns", []):
        if col not in df.columns:
            continue
        n = int(df[col].duplicated(keep=False).sum())
        if n > 0:
            examples = df[col][df[col].duplicated(keep=False)].dropna().unique()[:3].tolist()
            issues.append(Issue(
                column=col,
                issue_type="duplicate_ids",
                detail=f"{n} rows share duplicate ID values",
                examples=examples,
                severity="critical",
                recommended_action="investigate and de-duplicate or reassign IDs",
                confidence="high",
            ))
    return issues


def _detect_mixed_types(col: str, series: pd.Series, profile: dict) -> list[Issue]:
    if not profile.get("is_mixed_type"):
        return []
    tc = profile["type_counts"]
    present = {t: c for t, c in tc.items() if c > 0}
    if len(present) <= 1:
        return []
    detail = ", ".join(f"{t}={c}" for t, c in present.items())
    return [Issue(
        column=col,
        issue_type="mixed_types",
        detail=f"Column contains mixed types: {detail}",
        examples=_sample_non_null(series, 3),
        severity="warning",
        recommended_action="coerce to dominant type; flag unconvertable values",
        confidence="high",
    )]


_WORD_NUMBER_MAP: dict[str, int] = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
    "hundred": 100, "thousand": 1000,
}
_WORD_NUMBER_RE = re.compile(
    r"\b(" + "|".join(_WORD_NUMBER_MAP.keys()) + r")\b", re.IGNORECASE
)


def _detect_word_numbers(col: str, series: pd.Series, profile: dict) -> list[Issue]:
    if not profile.get("has_word_numbers"):
        return []
    # Build mask on the full series index to avoid alignment issues
    matches = series.apply(
        lambda v: bool(_WORD_NUMBER_RE.search(str(v))) if pd.notna(v) else False
    )
    bad_vals = series[matches].tolist()[:3]
    n = int(matches.sum())
    return [Issue(
        column=col,
        issue_type="word_numbers",
        detail=f"{n} value(s) written as text numbers",
        examples=bad_vals,
        severity="warning",
        recommended_action="convert word numbers to integers",
        confidence="high",
    )]


_CURRENCY_RE = re.compile(r"[£$€¥₹]")


def _detect_currency_strings(
    col: str, series: pd.Series, profile: dict, semantic: str
) -> list[Issue]:
    if not profile.get("has_currency_symbols"):
        return []
    matches = series.apply(
        lambda v: bool(_CURRENCY_RE.search(str(v))) if pd.notna(v) else False
    )
    n = int(matches.sum())
    bad_vals = series[matches].tolist()[:3]
    return [Issue(
        column=col,
        issue_type="currency_string",
        detail=f"{n} value(s) contain currency symbols",
        examples=bad_vals,
        severity="warning",
        recommended_action="strip currency symbols and commas, coerce to float",
        confidence="high",
    )]


def _detect_negative_ages(col: str, series: pd.Series, semantic: str) -> list[Issue]:
    if semantic != "age":
        return []
    numeric = pd.to_numeric(series, errors="coerce")
    neg = numeric[numeric < 0]
    if neg.empty:
        return []
    return [Issue(
        column=col,
        issue_type="negative_age",
        detail=f"{len(neg)} negative age value(s)",
        examples=neg.tolist()[:3],
        severity="critical",
        recommended_action="set negative ages to null",
        confidence="high",
    )]


def _detect_impossible_ages(col: str, series: pd.Series, semantic: str) -> list[Issue]:
    if semantic != "age":
        return []
    numeric = pd.to_numeric(series, errors="coerce")
    impossible = numeric[(numeric > 120) & numeric.notna()]
    if impossible.empty:
        return []
    return [Issue(
        column=col,
        issue_type="impossible_age",
        detail=f"{len(impossible)} age value(s) > 120",
        examples=impossible.tolist()[:3],
        severity="critical",
        recommended_action="set impossible ages to null",
        confidence="high",
    )]


_DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
    "%Y/%m/%d", "%d.%m.%Y", "%Y%m%d",
]


def _detect_invalid_dates(col: str, series: pd.Series, semantic: str) -> list[Issue]:
    if semantic != "date":
        return []
    str_vals = series.dropna().astype(str)
    if str_vals.empty:
        return []

    parsed = pd.to_datetime(str_vals, errors="coerce")
    n_invalid = int(parsed.isna().sum())
    if n_invalid == 0:
        return []

    bad_vals = str_vals[parsed.isna()].tolist()[:3]
    return [Issue(
        column=col,
        issue_type="invalid_date",
        detail=f"{n_invalid} value(s) cannot be parsed as dates",
        examples=bad_vals,
        severity="critical",
        recommended_action="set unparseable dates to null",
        confidence="high",
    )]


def _detect_mixed_date_formats(col: str, series: pd.Series, semantic: str) -> list[Issue]:
    if semantic != "date":
        return []
    str_vals = series.dropna().astype(str)
    if str_vals.empty:
        return []

    format_hits: dict[str, int] = {}
    for fmt in _DATE_FORMATS:
        try:
            parsed = pd.to_datetime(str_vals, format=fmt, errors="coerce")
            hit = int(parsed.notna().sum())
            if hit > 0:
                format_hits[fmt] = hit
        except Exception:
            pass

    if len(format_hits) <= 1:
        return []

    detail = "; ".join(f"{fmt}: {c}" for fmt, c in format_hits.items())
    return [Issue(
        column=col,
        issue_type="mixed_date_formats",
        detail=f"Multiple date formats detected — {detail}",
        examples=str_vals.tolist()[:3],
        severity="warning",
        recommended_action="standardise all dates to YYYY-MM-DD",
        confidence="medium",
    )]


def _detect_whitespace(col: str, series: pd.Series, profile: dict) -> list[Issue]:
    stats = profile.get("string_stats", {})
    if not stats.get("has_leading_trailing_spaces"):
        return []
    str_vals = series.dropna().astype(str)
    n = int(str_vals.str.strip().ne(str_vals).sum())
    return [Issue(
        column=col,
        issue_type="whitespace",
        detail=f"{n} value(s) have leading/trailing whitespace",
        examples=[],
        severity="info",
        recommended_action="strip whitespace",
        confidence="high",
    )]


def _detect_mixed_case(
    col: str, series: pd.Series, profile: dict, semantic: str
) -> list[Issue]:
    stats = profile.get("string_stats", {})
    if not stats.get("has_mixed_case"):
        return []
    # Only flag for categorical / name columns — mixed case in free text is fine
    if semantic not in ("unknown", "name", "country", "gender"):
        return []
    return [Issue(
        column=col,
        issue_type="mixed_case",
        detail="Column has inconsistent casing across values",
        examples=_sample_non_null(series, 3),
        severity="info",
        recommended_action="normalise to lowercase (or title-case for names/countries)",
        confidence="medium",
    )]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _sample_non_null(series: pd.Series, n: int) -> list[Any]:
    return series.dropna().head(n).tolist()
