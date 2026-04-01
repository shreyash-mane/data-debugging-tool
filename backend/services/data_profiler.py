"""
data_profiler.py — Column-level data quality profiling engine.

Architecture principle: DETECTION IS SEPARATE FROM CLEANING.
This module only inspects and reports. Cleaning is done by smart_cleaner.py.

Per-column detectors (each returns one issue-dict or None):
  _detect_nulls               — missing / NaN values
  _detect_null_like_strings   — "N/A", "not_available", "none" etc.
  _detect_word_numbers        — "thirty", "twenty-eight" etc.
  _detect_currency_symbols    — £45000, $1,200, €500
  _detect_mixed_types         — mix of numeric and text in same column
  _detect_negative_values     — negative in columns that must be positive
  _detect_impossible_range    — age>120, score>100 etc.
  _detect_whitespace          — leading / trailing spaces
  _detect_case_inconsistency  — "Male" / "male" / "MALE"
  _detect_mixed_date_formats  — values parsed with different format strings
  _detect_invalid_dates       — 2023-13-01, 2023-01-40 etc.
  _detect_outliers            — numeric values > 3 σ from mean

Dataset-level detectors:
  _detect_duplicate_rows      — exact full-row duplicates
  _detect_duplicate_ids       — duplicates in ID-like columns

Public API:
  infer_column_type(series, col_name)  → "numeric"|"date"|"categorical"|"text"|"id"
  generate_cleaning_report(df)         → full report for /suggest-cleaning
  apply_and_preview(df, config)        → cleaned preview for /apply-cleaning
"""

from __future__ import annotations

import re
from datetime import datetime as _dt
from typing import Any

import numpy as np
import pandas as pd


# ── Shared constants ───────────────────────────────────────────────────────────

NULL_LIKE_SET = frozenset({
    'not_available', 'not available', 'n/a', 'na', 'n.a', 'n.a.',
    'none', 'unknown', 'undefined', 'null', 'nil', 'missing',
    'nan', '#n/a', '#na', '-', '--', '---', 'not applicable',
    'not specified', 'unspecified', 'blank', 'empty', 'tbd', 'tbh',
})

CURRENCY_RE   = re.compile(r'[£$€¥₹]')
WHITESPACE_RE = re.compile(r'^\s+|\s+$')

WORD_TO_NUM: dict[str, int] = {
    'zero':0,'one':1,'two':2,'three':3,'four':4,'five':5,
    'six':6,'seven':7,'eight':8,'nine':9,'ten':10,
    'eleven':11,'twelve':12,'thirteen':13,'fourteen':14,'fifteen':15,
    'sixteen':16,'seventeen':17,'eighteen':18,'nineteen':19,
    'twenty':20,'thirty':30,'forty':40,'fifty':50,
    'sixty':60,'seventy':70,'eighty':80,'ninety':90,
    'hundred':100,'thousand':1000,
}

# Column-name keyword buckets for semantic detection
AGE_KW      = ('age','years','yr','yrs','years_old')
SCORE_KW    = ('score','rating','grade','mark','rank','points','gpa','percent')
SALARY_KW   = ('salary','wage','income','pay','compensation','earnings','revenue')
CURRENCY_KW = ('salary','wage','income','pay','price','cost','revenue',
               'profit','amount','fee','rate','budget','worth')
DATE_KW     = ('date','time','created','updated','login','signup','registered',
               'modified','timestamp','dob','birth','at','on','when')
ID_KW       = ('id','_id','uid','uuid','key','pk','ref','code')

# Date format strings to try when detecting mixed formats
DATE_FORMATS = [
    '%Y-%m-%d','%Y/%m/%d',             # ISO variants
    '%d/%m/%Y','%m/%d/%Y',             # DD/MM or MM/DD
    '%d-%m-%Y','%m-%d-%Y',             # DD-MM or MM-DD
    '%d-%m-%y','%m-%d-%y',             # 2-digit year
    '%d %b %Y','%b %d, %Y',            # text months
    '%d %B %Y','%B %d, %Y',
    '%B %d %Y','%d %B, %Y',
]

# Impossible-value bounds per semantic group
IMPOSSIBLE_BOUNDS: list[tuple[tuple[str,...], float|None, float|None]] = [
    (AGE_KW,    0.0, 120.0),
    (SCORE_KW,  0.0, 100.0),
    (SALARY_KW, 0.0, None),   # salary must be ≥ 0
]

MAX_EXAMPLES = 5   # max bad-value examples per issue


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_issue(
    column: str | None,
    issue_type: str,
    detail: str,
    examples: list,
    severity: str,
    recommended_action: str,
    count: int = 0,
) -> dict:
    return {
        "column":             column,
        "issue_type":         issue_type,
        "detail":             detail,
        "examples":           [str(e) for e in examples[:MAX_EXAMPLES]],
        "count":              count,
        "severity":           severity,   # "critical" | "warning" | "info"
        "recommended_action": recommended_action,
    }


def _try_parse(value: str, fmt: str) -> bool:
    try:
        _dt.strptime(value.strip(), fmt)
        return True
    except (ValueError, TypeError):
        return False


def _parse_word_number(text: str) -> float | None:
    """Parse 'thirty' → 30.0, 'twenty-eight' → 28.0, or None if not a word number."""
    text = text.lower().strip().replace('-', ' ')
    parts = text.split()
    if not parts or not all(p in WORD_TO_NUM for p in parts):
        return None
    total = current = 0
    for word in parts:
        val = WORD_TO_NUM[word]
        if val == 1000:
            current = (current or 1) * 1000
            total += current; current = 0
        elif val == 100:
            current = (current or 1) * 100
        elif val >= 20:
            current += val
        else:
            current += val
    total += current
    return float(total)


# ── Column type inference ──────────────────────────────────────────────────────

def infer_column_type(series: pd.Series, col_name: str) -> str:
    """
    Infer the semantic type of a column.
    Returns: "id" | "numeric" | "date" | "categorical" | "text"
    """
    cl = col_name.lower().replace(' ', '_')
    non_null = series.dropna()
    n = len(non_null)

    # Already a numeric dtype
    if pd.api.types.is_numeric_dtype(series.dtype):
        # Check if it looks like an ID (all integers, high cardinality)
        if any(kw in cl for kw in ID_KW) and n > 0:
            return "id"
        return "numeric"

    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return "date"

    if n == 0:
        return "text"

    sample_str = non_null.astype(str).str.strip()

    # ID column heuristic: name contains id keyword + near-unique values
    if any(kw in cl for kw in ID_KW):
        uniq_ratio = series.nunique(dropna=True) / max(n, 1)
        if uniq_ratio > 0.8:
            return "id"

    # Date: name hint + >50% parse as a date
    if any(kw in cl for kw in DATE_KW) and len(sample_str) > 0:
        parsed = pd.to_datetime(sample_str.head(50), errors='coerce', infer_datetime_format=True)
        if parsed.notna().mean() > 0.5:
            return "date"

    # Numeric after symbol stripping: >70% parseable
    stripped = sample_str.head(100).str.replace(r'[£$€¥₹,\s%]', '', regex=True)
    num_ok = pd.to_numeric(stripped, errors='coerce').notna().mean()
    if num_ok > 0.7:
        return "numeric"

    # Categorical vs text by cardinality + string length
    uniq_ratio = series.nunique(dropna=True) / max(n, 1)
    avg_len = sample_str.str.len().mean() if len(sample_str) else 0
    if uniq_ratio > 0.6 or avg_len > 60:
        return "text"

    return "categorical"


# ── Per-column detectors ───────────────────────────────────────────────────────

def _detect_nulls(series: pd.Series, col: str) -> dict | None:
    n_total = len(series)
    n_null  = int(series.isna().sum())
    if n_null == 0:
        return None
    pct = n_null / n_total * 100
    sev = "critical" if pct >= 40 else "warning" if pct > 10 else "info"
    return _make_issue(
        col, "missing_values",
        f"{n_null} missing values ({pct:.1f}% of column)",
        [],  # no "bad value" examples for nulls — they are absence of values
        sev,
        "Fill with median (numeric) / mode (categorical) after other fixes, "
        "or drop column if ≥50% missing",
        count=n_null,
    )


def _detect_null_like_strings(series: pd.Series, col: str) -> dict | None:
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str)
    mask = non_null.str.strip().str.lower().isin(NULL_LIKE_SET)
    bad  = non_null[mask].tolist()
    n    = len(bad)
    if n == 0:
        return None
    return _make_issue(
        col, "null_like_strings",
        f"{n} value(s) are text placeholders for null",
        sorted(set(bad)),
        "warning",
        "Replace these strings with proper NaN before analysis",
        count=n,
    )


def _detect_word_numbers(series: pd.Series, col: str) -> dict | None:
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str)
    bad = [v for v in non_null if _parse_word_number(v) is not None]
    n   = len(bad)
    if n == 0:
        return None
    return _make_issue(
        col, "word_numbers",
        f"{n} value(s) are written as English words instead of digits",
        sorted(set(bad)),
        "warning",
        "Convert to numeric (e.g. 'thirty' → 30, 'twenty-eight' → 28)",
        count=n,
    )


def _detect_currency_symbols(series: pd.Series, col: str) -> dict | None:
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str)
    mask = non_null.str.contains(CURRENCY_RE, regex=True)
    bad  = non_null[mask].tolist()
    n    = len(bad)
    if n == 0:
        return None
    return _make_issue(
        col, "currency_symbols",
        f"{n} value(s) contain currency symbols (£ $ € ¥ ₹)",
        sorted(set(bad)),
        "warning",
        "Strip currency symbols and commas, then cast column to float",
        count=n,
    )


def _detect_mixed_types(series: pd.Series, col: str) -> dict | None:
    """Detect columns where most values look numeric but some are plain text."""
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str).str.strip()
    n = len(non_null)
    if n == 0:
        return None
    # Strip currency/symbols and try numeric parse
    stripped  = non_null.str.replace(r'[£$€¥₹,\s%]', '', regex=True)
    parseable = pd.to_numeric(stripped, errors='coerce').notna()
    n_num = int(parseable.sum())
    n_txt = n - n_num
    # Only flag if some are numeric and some are not — avoid pure-text columns
    if n_num == 0 or n_txt == 0:
        return None
    # Don't flag if text values are just word numbers (handled separately)
    text_vals = non_null[~parseable]
    non_word  = [v for v in text_vals if _parse_word_number(v) is None]
    n_bad = len(non_word)
    if n_bad == 0:
        return None
    return _make_issue(
        col, "mixed_types",
        f"Column mixes {n_num} numeric and {n_bad} non-numeric values",
        non_word[:MAX_EXAMPLES],
        "warning",
        "Coerce to numeric (errors='coerce') to convert text to NaN, then impute",
        count=n_bad,
    )


def _detect_negative_values(series: pd.Series, col: str) -> dict | None:
    """Detect negative values in columns that should be non-negative."""
    if not pd.api.types.is_numeric_dtype(series.dtype):
        return None
    cl = col.lower()
    if not any(any(kw in cl for kw in group) for group, *_ in IMPOSSIBLE_BOUNDS):
        return None
    neg_mask = series < 0
    bad = series[neg_mask].dropna().tolist()
    n   = len(bad)
    if n == 0:
        return None
    return _make_issue(
        col, "negative_values",
        f"{n} impossible negative value(s) in a column that must be ≥ 0",
        [round(v, 4) for v in bad],
        "critical",
        "Replace negative values with NaN (they are data entry errors), then impute",
        count=n,
    )


def _detect_impossible_range(series: pd.Series, col: str) -> list[dict]:
    """Detect values outside the semantically valid range for this column."""
    if not pd.api.types.is_numeric_dtype(series.dtype):
        return []
    cl = col.lower()
    issues: list[dict] = []
    for keywords, lo, hi in IMPOSSIBLE_BOUNDS:
        if not any(kw in cl for kw in keywords):
            continue
        # Too-large values
        if hi is not None:
            over  = series[series > hi].dropna()
            if len(over):
                issues.append(_make_issue(
                    col, "impossible_range_high",
                    f"{len(over)} value(s) exceed the valid maximum of {hi}",
                    [round(v, 4) for v in over.tolist()],
                    "critical",
                    f"Replace values > {hi} with NaN (data entry errors)",
                    count=len(over),
                ))
        # Too-small values (non-negative check; _detect_negative_values handles < 0)
        if lo is not None and lo > 0:
            under = series[(series < lo) & (series >= 0)].dropna()
            if len(under):
                issues.append(_make_issue(
                    col, "impossible_range_low",
                    f"{len(under)} value(s) are below the valid minimum of {lo}",
                    [round(v, 4) for v in under.tolist()],
                    "warning",
                    f"Replace values < {lo} with NaN",
                    count=len(under),
                ))
    return issues


def _detect_whitespace(series: pd.Series, col: str) -> dict | None:
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str)
    has_ws   = non_null[non_null != non_null.str.strip()]
    n        = len(has_ws)
    if n == 0:
        return None
    return _make_issue(
        col, "whitespace",
        f"{n} value(s) have leading or trailing whitespace",
        has_ws.tolist(),
        "info",
        "Strip whitespace from all string values (.str.strip())",
        count=n,
    )


def _detect_case_inconsistency(series: pd.Series, col: str) -> dict | None:
    """Detect the same word written in multiple capitalisation styles."""
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str).str.strip()
    if len(non_null) == 0:
        return None
    lowered  = non_null.str.lower()
    # Group original values by their lower-case form
    groups: dict[str, set[str]] = {}
    for orig, low in zip(non_null, lowered):
        groups.setdefault(low, set()).add(orig)
    # Find groups with more than one distinct capitalisation
    inconsistent: list[str] = []
    for low, originals in groups.items():
        if len(originals) > 1:
            inconsistent.extend(sorted(originals))
    n = len(inconsistent)
    if n == 0:
        return None
    return _make_issue(
        col, "case_inconsistency",
        f"The same value appears in {n} different capitalisations",
        inconsistent,
        "warning",
        "Normalise to lowercase (or title case) so grouping/filtering works correctly",
        count=n,
    )


def _detect_mixed_date_formats(series: pd.Series, col: str) -> dict | None:
    """Detect when different rows use different date format strings."""
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str).str.strip()
    if len(non_null) == 0:
        return None

    # Map each sample value to the format(s) that parse it
    sample = non_null.head(200).tolist()
    format_counts: dict[str, int] = {}
    value_formats: dict[str, str] = {}

    for v in sample:
        matched: list[str] = []
        for fmt in DATE_FORMATS:
            if _try_parse(v, fmt):
                matched.append(fmt)
        if matched:
            fmt0 = matched[0]
            format_counts[fmt0] = format_counts.get(fmt0, 0) + 1
            value_formats[v] = fmt0

    if len(format_counts) <= 1:
        return None  # all values share one format (or none parsed)

    # Find examples that use a non-dominant format
    dominant = max(format_counts, key=format_counts.get)  # type: ignore[arg-type]
    examples = [v for v, f in value_formats.items() if f != dominant]
    if not examples:
        return None

    return _make_issue(
        col, "mixed_date_formats",
        f"Dates use {len(format_counts)} different format patterns "
        f"(dominant: '{dominant}')",
        examples,
        "warning",
        "Standardise all dates to ISO YYYY-MM-DD using pandas to_datetime",
        count=len(examples),
    )


def _detect_invalid_dates(series: pd.Series, col: str) -> dict | None:
    """Detect values that look date-like but cannot be parsed as valid dates."""
    if series.dtype != object:
        return None
    non_null = series.dropna().astype(str).str.strip()
    if len(non_null) == 0:
        return None

    # Only check columns that have at least some valid-looking dates
    parsed = pd.to_datetime(non_null.head(100), errors='coerce', infer_datetime_format=True)
    if parsed.notna().mean() < 0.3:
        return None  # not really a date column

    # All non-null values: those that fail parsing are "invalid dates"
    all_parsed = pd.to_datetime(non_null, errors='coerce', infer_datetime_format=True)
    invalid = non_null[all_parsed.isna()].tolist()
    n = len(invalid)
    if n == 0:
        return None
    return _make_issue(
        col, "invalid_dates",
        f"{n} value(s) look date-like but cannot be parsed "
        f"(e.g. month>12 or day>31)",
        invalid,
        "critical",
        "Replace invalid dates with NaN (pd.to_datetime errors='coerce')",
        count=n,
    )


def _detect_outliers(series: pd.Series, col: str) -> dict | None:
    """Detect numeric values more than 3 standard deviations from the mean."""
    if not pd.api.types.is_numeric_dtype(series.dtype):
        return None
    non_null = series.dropna()
    if len(non_null) < 10:
        return None
    mean = float(non_null.mean())
    std  = float(non_null.std())
    if std == 0:
        return None
    z_scores = (non_null - mean).abs() / std
    outliers  = non_null[z_scores > 3].tolist()
    n = len(outliers)
    if n == 0:
        return None
    return _make_issue(
        col, "outliers",
        f"{n} value(s) are more than 3 standard deviations from the mean "
        f"(mean={mean:.2f}, std={std:.2f})",
        [round(v, 4) for v in outliers],
        "warning",
        "Review outliers — cap, remove, or investigate before analysis",
        count=n,
    )


# ── Dataset-level detectors ────────────────────────────────────────────────────

def _detect_duplicate_rows(df: pd.DataFrame) -> dict | None:
    n_dup = int(df.duplicated().sum())
    if n_dup == 0:
        return None
    pct = n_dup / len(df) * 100
    return _make_issue(
        None, "duplicate_rows",
        f"{n_dup} exact duplicate row(s) ({pct:.1f}% of dataset)",
        [],
        "critical" if pct > 10 else "warning",
        "Remove duplicates with drop_duplicates(keep='first')",
        count=n_dup,
    )


def _detect_duplicate_ids(df: pd.DataFrame) -> list[dict]:
    """Detect duplicated values in columns that look like unique identifiers."""
    issues: list[dict] = []
    for col in df.columns:
        cl  = col.lower().replace(' ', '_')
        if not any(kw in cl for kw in ID_KW):
            continue
        series  = df[col].dropna()
        n_dup   = int(series.duplicated().sum())
        if n_dup == 0:
            continue
        dup_vals = series[series.duplicated(keep=False)].unique().tolist()
        issues.append(_make_issue(
            col, "duplicate_ids",
            f"{n_dup} duplicate value(s) in what appears to be an ID column",
            [str(v) for v in dup_vals],
            "critical",
            "Investigate duplicate IDs — deduplicate or re-key",
            count=n_dup,
        ))
    return issues


# ── Column type grouping ───────────────────────────────────────────────────────

def _group_columns_by_type(df: pd.DataFrame) -> dict[str, list[str]]:
    """Group all columns into semantic type buckets for the suggested_config."""
    groups: dict[str, list[str]] = {
        "id_columns":          [],
        "numeric_columns":     [],
        "date_columns":        [],
        "categorical_columns": [],
        "text_columns":        [],
    }
    for col in df.columns:
        ct = infer_column_type(df[col], col)
        key = ct + "_columns"
        groups.setdefault(key, []).append(col)
    return groups


# ── Smart-clean config suggestion ─────────────────────────────────────────────

def _suggest_smart_clean_config(df: pd.DataFrame) -> dict:
    """
    Produce the config dict to pass to smart_clean_dataframe.
    Groups columns by their role: age / score / currency / salary / date.
    """
    age_cols:      list[str] = []
    score_cols:    list[str] = []
    currency_cols: list[str] = []
    salary_cols:   list[str] = []
    date_cols:     list[str] = []

    for col in df.columns:
        cl = col.lower().replace(' ', '_')
        ct = infer_column_type(df[col], col)

        if any(kw in cl for kw in AGE_KW):
            age_cols.append(col)
        if any(kw in cl for kw in SCORE_KW):
            score_cols.append(col)
        if any(kw in cl for kw in SALARY_KW):
            salary_cols.append(col)

        # Currency: by name OR by content (£$€ symbols found)
        if any(kw in cl for kw in CURRENCY_KW):
            currency_cols.append(col)
        elif df[col].dtype == object:
            non_null = df[col].dropna().astype(str)
            if len(non_null) and non_null.str.contains(CURRENCY_RE, regex=True).mean() > 0.05:
                currency_cols.append(col)

        if ct == "date":
            date_cols.append(col)

    return {
        "age_columns":      age_cols,
        "score_columns":    score_cols,
        "currency_columns": list(set(currency_cols)),
        "salary_columns":   salary_cols,
        "date_columns":     date_cols,
    }


# ── Main profiling function ────────────────────────────────────────────────────

def generate_cleaning_report(df: pd.DataFrame) -> dict:
    """
    Full data-quality report for a DataFrame.

    Returns a dict matching the /suggest-cleaning API contract:
    {
        "suggested_config": { ... },         # for smart_clean step
        "column_types":     { ... },         # grouped by semantic type
        "issues":           [ ... ],         # per-issue dicts with examples
        "summary": {
            "total_rows":          int,
            "total_columns":       int,
            "total_issues":        int,
            "critical":            int,
            "warning":             int,
            "info":                int,
            "columns_with_issues": int,
            "duplicate_rows":      int,
        }
    }
    """
    issues: list[dict] = []

    # ── Dataset-level issues ──────────────────────────────────────────────────
    dup_issue = _detect_duplicate_rows(df)
    if dup_issue:
        issues.append(dup_issue)

    issues.extend(_detect_duplicate_ids(df))

    # ── Per-column issues ─────────────────────────────────────────────────────
    for col in df.columns:
        series = df[col]
        ct     = infer_column_type(series, col)

        # Every column: nulls and null-like strings
        _add(issues, _detect_nulls(series, col))
        if series.dtype == object:
            _add(issues, _detect_null_like_strings(series, col))
            _add(issues, _detect_whitespace(series, col))

        # Numeric-detected columns (may still be stored as object)
        if ct in ("numeric", "id"):
            _add(issues, _detect_word_numbers(series, col))
            _add(issues, _detect_currency_symbols(series, col))
            _add(issues, _detect_mixed_types(series, col))

        # After type coercion, check numeric-only issues
        # Try to get a truly numeric view for bounds / outlier checks
        if pd.api.types.is_numeric_dtype(series.dtype):
            _add(issues, _detect_negative_values(series, col))
            issues.extend(_detect_impossible_range(series, col))
            _add(issues, _detect_outliers(series, col))
        elif ct == "numeric":
            # Column LOOKS numeric but is stored as object — coerce and re-check
            stripped = series.astype(str).str.replace(r'[£$€¥₹,\s%]', '', regex=True)
            coerced  = pd.to_numeric(stripped, errors='coerce')
            if coerced.notna().mean() > 0.5:
                _add(issues, _detect_negative_values(coerced, col))
                issues.extend(_detect_impossible_range(coerced, col))

        # Date columns
        if ct == "date" or series.dtype == object:
            _add(issues, _detect_mixed_date_formats(series, col))
            _add(issues, _detect_invalid_dates(series, col))

        # Categorical columns
        if ct == "categorical":
            _add(issues, _detect_case_inconsistency(series, col))

    # ── Build summary ─────────────────────────────────────────────────────────
    dup_rows = dup_issue["count"] if dup_issue else 0
    critical = sum(1 for i in issues if i["severity"] == "critical")
    warning  = sum(1 for i in issues if i["severity"] == "warning")
    info     = sum(1 for i in issues if i["severity"] == "info")
    cols_affected = len({i["column"] for i in issues if i["column"]})

    return {
        "suggested_config": _suggest_smart_clean_config(df),
        "column_types":     _group_columns_by_type(df),
        "issues":           issues,
        "summary": {
            "total_rows":          len(df),
            "total_columns":       len(df.columns),
            "total_issues":        len(issues),
            "critical":            critical,
            "warning":             warning,
            "info":                info,
            "columns_with_issues": cols_affected,
            "duplicate_rows":      dup_rows,
        },
    }


def _add(lst: list, item: dict | None) -> None:
    if item is not None:
        lst.append(item)


# ── Apply-cleaning preview ─────────────────────────────────────────────────────

def apply_and_preview(df: pd.DataFrame, config: dict, preview_rows: int = 50) -> dict:
    """
    Run smart_clean_dataframe with the given config and return a cleaned preview.

    Returns:
    {
        "before": { rows, cols, total_nulls, duplicate_rows },
        "after":  { rows, cols, total_nulls, duplicate_rows },
        "changes": { ... },      # smart_clean summary log
        "cleaned_preview": [...] # first preview_rows rows as list-of-dicts
    }
    """
    from services.smart_cleaner import smart_clean_dataframe
    from services.csv_service import sample_rows

    before_nulls = int(df.isna().sum().sum())
    before_dups  = int(df.duplicated().sum())

    cleaned_df, log = smart_clean_dataframe(df, config)

    after_nulls = int(cleaned_df.isna().sum().sum())
    after_dups  = int(cleaned_df.duplicated().sum())

    # Produce a JSON-safe preview
    preview_df = cleaned_df.head(preview_rows).copy()
    preview    = sample_rows(preview_df, n=preview_rows)

    return {
        "before": {
            "rows":           len(df),
            "cols":           len(df.columns),
            "total_nulls":    before_nulls,
            "duplicate_rows": before_dups,
        },
        "after": {
            "rows":           len(cleaned_df),
            "cols":           len(cleaned_df.columns),
            "total_nulls":    after_nulls,
            "duplicate_rows": after_dups,
        },
        "changes":         log.get("summary", {}),
        "step_details":    log.get("steps", {}),
        "columns_dropped": log.get("steps", {}).get("7_high_null_cols_dropped", []),
        "cleaned_preview": preview,
    }
