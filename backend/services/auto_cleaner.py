"""
auto_cleaner.py — Intelligent automatic data cleaning engine.

Handles:
  - Column type detection (numeric, categorical, datetime, text)
  - Symbol/currency stripping and safe numeric conversion
  - Mixed datetime format normalization
  - Whitespace trimming for text/categorical
  - Missing value imputation (skewness-based for numeric, mode/Unknown for categorical,
    median date for datetime)
  - Issue detection (nulls, duplicates, negative values, invalid emails, mixed types)
  - Per-column explanations with full reasoning chain

Main API:
  auto_clean_dataframe(df, config) -> (cleaned_df, report)
  build_auto_clean_explanations(report, anomalies, diff) -> list[dict]
"""

from __future__ import annotations

import math
import re
import numpy as np
import pandas as pd
from typing import Any

# ── Constants ──────────────────────────────────────────────────────────────────

MISSING_DROP_THRESHOLD = 0.40    # drop numeric column if > 40% missing
SKEW_MEAN_THRESHOLD = 0.5        # |skew| < 0.5 → mean; else → median
HIGH_UNIQUE_RATIO = 0.5          # unique/non-null > 0.5 → high cardinality
HIGH_MISSING_CATEGORICAL = 0.40  # > 40% missing in categorical → fill "Unknown"
LOW_UNIQUE_MAX = 20              # ≤ 20 uniques → low cardinality (mode eligible)

NUMERIC_SYMBOLS_RE = re.compile(r"[\$£€¥₹,\s%]")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

POSITIVE_COLUMN_KEYWORDS = [
    "age", "salary", "price", "cost", "count", "quantity",
    "amount", "score", "rate", "revenue", "profit", "weight",
    "height", "distance", "size",
]


# ── Column type detection ──────────────────────────────────────────────────────

def detect_column_type(series: pd.Series) -> str:
    """
    Detect the semantic type of a column.
    Returns: "numeric" | "categorical" | "datetime" | "text"
    """
    dtype = series.dtype

    if pd.api.types.is_numeric_dtype(dtype):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"

    if dtype == object or pd.api.types.is_string_dtype(dtype):
        sample = series.dropna().astype(str)
        if len(sample) == 0:
            return "text"

        # Try datetime heuristic on first 100 values
        try:
            converted = pd.to_datetime(sample.head(100), dayfirst=False, errors="coerce")
            if converted.notna().mean() > 0.7:
                return "datetime"
        except Exception:
            pass

        # Try numeric after stripping symbols
        cleaned_sample = sample.head(100).str.replace(NUMERIC_SYMBOLS_RE, "", regex=True).str.strip()
        num_conv = pd.to_numeric(cleaned_sample, errors="coerce")
        if num_conv.notna().mean() > 0.7:
            return "numeric"

        # Text vs categorical by cardinality and string length
        n_non_null = max(series.notna().sum(), 1)
        unique_ratio = series.nunique(dropna=True) / n_non_null
        avg_len = sample.str.len().mean() if len(sample) else 0
        if unique_ratio > HIGH_UNIQUE_RATIO or avg_len > 50:
            return "text"

        return "categorical"

    return "text"


# ── Per-type cleaners ──────────────────────────────────────────────────────────

def _clean_numeric_series(series: pd.Series) -> tuple[pd.Series, list[str]]:
    """Strip currency/formatting symbols and convert to numeric."""
    notes: list[str] = []

    if pd.api.types.is_numeric_dtype(series.dtype):
        return series, notes

    str_series = series.astype(str)
    stripped = str_series.str.replace(NUMERIC_SYMBOLS_RE, "", regex=True).str.strip()

    # Count how many values had symbols stripped
    non_null_mask = series.notna()
    n_stripped = int((str_series[non_null_mask] != stripped[non_null_mask]).sum())
    if n_stripped > 0:
        notes.append(f"Stripped currency/formatting symbols from {n_stripped} value(s)")

    converted = pd.to_numeric(stripped, errors="coerce")

    # How many became NaN that weren't before?
    new_nulls = int(converted.isna().sum()) - int(series.isna().sum())
    if new_nulls > 0:
        notes.append(f"{new_nulls} non-numeric value(s) could not be converted → set to NaN")

    return converted, notes


def _clean_datetime_series(series: pd.Series) -> tuple[pd.Series, list[str]]:
    """Normalise mixed datetime formats; invalid → NaT."""
    notes: list[str] = []

    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return series, notes

    converted = pd.to_datetime(series, dayfirst=False, errors="coerce")
    new_nats = int(converted.isna().sum()) - int(series.isna().sum())
    if new_nats > 0:
        notes.append(f"{new_nats} value(s) could not be parsed as dates → set to NaT")

    return converted, notes


def _clean_text_series(series: pd.Series) -> tuple[pd.Series, list[str]]:
    """Trim leading/trailing whitespace from object/string columns."""
    notes: list[str] = []

    if not (series.dtype == object or pd.api.types.is_string_dtype(series.dtype)):
        return series, notes

    non_null = series.dropna().astype(str)
    n_trimmed = int((non_null != non_null.str.strip()).sum())
    if n_trimmed > 0:
        notes.append(f"Trimmed whitespace from {n_trimmed} value(s)")

    cleaned = series.copy()
    mask = series.notna()
    cleaned[mask] = series[mask].astype(str).str.strip()
    return cleaned, notes


# ── Missing value imputation ───────────────────────────────────────────────────

def _impute_numeric(
    series: pd.Series, col_name: str
) -> tuple[pd.Series, str, str, dict]:
    """
    Auto-impute a numeric column using skewness heuristic.
    Returns (imputed_series, method, reason, stats).
    method can be: "none" | "mean" | "median" | "drop_column"
    """
    total = len(series)
    missing = int(series.isna().sum())
    missing_pct = missing / total if total else 0

    if missing_pct > MISSING_DROP_THRESHOLD:
        return series, "drop_column", (
            f"Missing rate {missing_pct*100:.1f}% exceeds 40% threshold — column dropped"
        ), {"missing_count": missing, "missing_pct": round(missing_pct * 100, 2)}

    if missing == 0:
        return series, "none", "No missing values", {}

    non_null = series.dropna()
    skewness = float(non_null.skew()) if len(non_null) >= 3 else 0.0

    if abs(skewness) < SKEW_MEAN_THRESHOLD:
        fill_val = float(non_null.mean())
        method = "mean"
        reason = f"Symmetric distribution (skewness = {skewness:.2f}) → MEAN is appropriate"
    else:
        fill_val = float(non_null.median())
        method = "median"
        reason = f"Skewed distribution (skewness = {skewness:.2f}) → MEDIAN is more robust"

    return series.fillna(fill_val), method, reason, {
        "missing_count": missing,
        "missing_pct": round(missing_pct * 100, 2),
        "skewness": round(skewness, 4),
        "fill_value": round(fill_val, 6),
    }


def _impute_categorical(
    series: pd.Series, col_name: str
) -> tuple[pd.Series, str, str, dict]:
    """Auto-impute a categorical/text column using mode or 'Unknown'."""
    total = len(series)
    missing = int(series.isna().sum())
    missing_pct = missing / total if total else 0

    if missing == 0:
        return series, "none", "No missing values", {}

    unique_count = int(series.nunique(dropna=True))

    if missing_pct > HIGH_MISSING_CATEGORICAL or unique_count > LOW_UNIQUE_MAX:
        fill_val = "Unknown"
        method = "constant"
        reason = (
            f"High missing rate ({missing_pct*100:.1f}%) or high cardinality "
            f"({unique_count} unique values) → filled with 'Unknown'"
        )
    else:
        mode_vals = series.mode(dropna=True)
        if len(mode_vals) > 0:
            fill_val = mode_vals[0]
            method = "mode"
            reason = (
                f"Low cardinality ({unique_count} unique values) → "
                f"filled with most frequent value: '{fill_val}'"
            )
        else:
            fill_val = "Unknown"
            method = "constant"
            reason = "No mode found → filled with 'Unknown'"

    return series.fillna(fill_val), method, reason, {
        "missing_count": missing,
        "missing_pct": round(missing_pct * 100, 2),
        "fill_value": str(fill_val),
        "unique_count": unique_count,
    }


def _impute_datetime(
    series: pd.Series, col_name: str
) -> tuple[pd.Series, str, str, dict]:
    """Auto-impute a datetime column using median date."""
    missing = int(series.isna().sum())
    if missing == 0:
        return series, "none", "No missing values", {}

    non_null = series.dropna()
    if len(non_null) == 0:
        return series, "left_missing", "No valid dates to compute median — left as NaT", {
            "missing_count": missing
        }

    median_date = non_null.sort_values().iloc[len(non_null) // 2]
    reason = f"Filled with median date ({str(median_date)[:10]})"
    return series.fillna(median_date), "median_date", reason, {
        "missing_count": missing,
        "missing_pct": round(missing / len(series) * 100, 2),
        "fill_value": str(median_date)[:10],
    }


# ── Issue detection ────────────────────────────────────────────────────────────

def _detect_column_issues(series: pd.Series, col_name: str, col_type: str) -> list[dict]:
    """Detect data quality issues in a single column."""
    issues: list[dict] = []
    total = len(series)

    # Missing values
    missing = int(series.isna().sum())
    if missing > 0:
        pct = missing / total * 100
        severity = "critical" if pct > 40 else "warning" if pct > 10 else "info"
        issues.append({
            "type": "missing_values",
            "severity": severity,
            "message": f"{missing} missing values ({pct:.1f}%)",
            "count": missing,
        })

    # Negative values in likely-positive columns
    if col_type == "numeric" and pd.api.types.is_numeric_dtype(series.dtype):
        if any(kw in col_name.lower() for kw in POSITIVE_COLUMN_KEYWORDS):
            neg = int((series < 0).sum())
            if neg > 0:
                issues.append({
                    "type": "negative_values",
                    "severity": "warning",
                    "message": f"{neg} negative value(s) in likely-positive column",
                    "count": neg,
                })

    # Invalid email format
    if col_type in ("categorical", "text") and any(
        kw in col_name.lower() for kw in ("email", "mail", "e_mail")
    ):
        non_null = series.dropna().astype(str)
        invalid = int(non_null[~non_null.str.match(EMAIL_RE)].count())
        if invalid > 0:
            issues.append({
                "type": "invalid_email",
                "severity": "warning",
                "message": f"{invalid} value(s) do not match email format",
                "count": invalid,
            })

    # Mixed types (object column with partial numeric content)
    if series.dtype == object:
        sample = series.dropna().astype(str).head(200)
        n_total = len(sample)
        if n_total > 0:
            n_numeric = int(pd.to_numeric(sample, errors="coerce").notna().sum())
            if 0 < n_numeric < n_total * 0.9 and n_numeric > n_total * 0.1:
                issues.append({
                    "type": "mixed_types",
                    "severity": "warning",
                    "message": (
                        f"Column contains mixed numeric and non-numeric values "
                        f"({n_numeric}/{n_total} look numeric)"
                    ),
                    "count": n_numeric,
                })

    return issues


def _detect_global_issues(df: pd.DataFrame) -> list[dict]:
    """Detect dataset-level quality issues."""
    issues: list[dict] = []
    n = len(df)
    if n == 0:
        return issues

    dup = int(df.duplicated().sum())
    if dup > 0:
        pct = dup / n * 100
        issues.append({
            "type": "duplicate_rows",
            "severity": "critical" if pct > 10 else "warning",
            "message": f"{dup} duplicate row(s) detected ({pct:.1f}%)",
            "count": dup,
        })

    for col in df.columns:
        null_pct = df[col].isna().mean()
        if null_pct > 0.80:
            issues.append({
                "type": "mostly_null_column",
                "severity": "critical",
                "message": f"Column '{col}' is {null_pct*100:.1f}% null",
                "column": col,
                "count": int(df[col].isna().sum()),
            })

    return issues


# ── Main engine ────────────────────────────────────────────────────────────────

def auto_clean_dataframe(
    df: pd.DataFrame,
    config: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Automatically clean a DataFrame — no user decisions required.

    config keys:
      - columns: list[str] | None   → columns to process (None = all)
      - drop_columns_above_threshold: bool  → actually drop >40% null columns (default True)

    Returns:
      (cleaned_df, report)

    report keys:
      - columns: list of per-column reports
      - global_issues: list of dataset-level issues
      - transformations_applied: flat list of all changes made
      - columns_dropped: list of column names dropped
      - summary: {total_columns_processed, columns_dropped, transformations_count}
    """
    config = config or {}
    target_cols = config.get("columns") or list(df.columns)
    do_drop = config.get("drop_columns_above_threshold", True)

    df = df.copy()
    column_reports: list[dict] = []
    transformations: list[dict] = []
    cols_to_drop: list[str] = []

    for col in target_cols:
        if col not in df.columns:
            continue

        series = df[col]
        col_rep: dict[str, Any] = {
            "column": col,
            "detected_type": None,
            "issues_before_clean": [],
            "cleaning_steps": [],
            "imputation": None,
            "final_null_count": 0,
        }

        # ── 1. Detect type ────────────────────────────────────────────────────
        col_type = detect_column_type(series)
        col_rep["detected_type"] = col_type

        # ── 2. Type-specific symbol/format cleaning ───────────────────────────
        if col_type == "numeric":
            cleaned, notes = _clean_numeric_series(series)
        elif col_type == "datetime":
            cleaned, notes = _clean_datetime_series(series)
        else:
            cleaned, notes = _clean_text_series(series)

        if notes:
            col_rep["cleaning_steps"].extend(notes)
            transformations.append({
                "column": col,
                "action": f"{col_type}_clean",
                "detail": "; ".join(notes),
            })
        df[col] = cleaned
        series = df[col]

        # ── 3. Detect issues on cleaned data ─────────────────────────────────
        col_rep["issues_before_clean"] = _detect_column_issues(series, col, col_type)

        # ── 4. Impute missing values ──────────────────────────────────────────
        if col_type == "numeric":
            imputed, method, reason, stats = _impute_numeric(series, col)
        elif col_type == "datetime":
            imputed, method, reason, stats = _impute_datetime(series, col)
        else:
            imputed, method, reason, stats = _impute_categorical(series, col)

        if method == "drop_column":
            col_rep["imputation"] = {
                "decision": "DROP_COLUMN",
                "method": "drop_column",
                "reason": reason,
                **stats,
            }
            if do_drop:
                cols_to_drop.append(col)
                transformations.append({
                    "column": col,
                    "action": "drop_column",
                    "detail": reason,
                })
        elif method not in ("none", "left_missing"):
            df[col] = imputed
            col_rep["imputation"] = {
                "decision": method.upper(),
                "method": method,
                "reason": reason,
                **stats,
            }
            transformations.append({
                "column": col,
                "action": f"fill_missing_{method}",
                "detail": (
                    f"Filled {stats.get('missing_count', 0)} missing value(s) "
                    f"using {method.upper()} — {reason}"
                ),
            })

        col_rep["final_null_count"] = int(df[col].isna().sum()) if col not in cols_to_drop else None
        column_reports.append(col_rep)

    # ── Drop columns flagged above threshold ──────────────────────────────────
    if cols_to_drop:
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    global_issues = _detect_global_issues(df)

    report = {
        "columns": column_reports,
        "global_issues": global_issues,
        "transformations_applied": transformations,
        "columns_dropped": cols_to_drop,
        "summary": {
            "total_columns_processed": len(target_cols),
            "columns_dropped": len(cols_to_drop),
            "transformations_count": len(transformations),
        },
    }

    return df, report


# ── Explanation builder ────────────────────────────────────────────────────────

def build_auto_clean_explanations(
    report: dict,
    anomalies: list[dict],
    diff: dict,
) -> list[dict]:
    """
    Convert an auto_clean report into rich per-column explanation dicts,
    compatible with the ExplanationPanel format.
    """
    explanations: list[dict] = []

    for col_rep in report.get("columns", []):
        col = col_rep["column"]
        col_type = col_rep.get("detected_type", "unknown")
        cleaning_steps = col_rep.get("cleaning_steps", [])
        issues = col_rep.get("issues_before_clean", [])
        imputation = col_rep.get("imputation")

        # Determine severity
        drop = imputation and imputation.get("decision") == "DROP_COLUMN"
        severity = "warning" if drop else "info"
        if any(i["severity"] == "critical" for i in issues):
            severity = "warning"

        # Build summary
        if drop:
            summary = f"Column '{col}' DROPPED — >{int(MISSING_DROP_THRESHOLD*100)}% missing"
            likely_cause = imputation.get("reason", "")
            suggested_fix = "Review source data for this column — consider re-collecting"
        elif imputation and imputation.get("decision") != "none":
            m = imputation.get("decision", "")
            cnt = imputation.get("missing_count", 0)
            pct = imputation.get("missing_pct", 0)
            fv = imputation.get("fill_value", "")
            summary = f"Column: {col} — {cnt} missing ({pct}%) filled via {m}"
            likely_cause = (
                f"[{col_type.upper()}] {imputation.get('reason', '')}"
            )
            extra = ""
            if "skewness" in imputation:
                extra = f" | Skewness: {imputation['skewness']}"
            suggested_fix = f"Decision: {m} — fill value: {fv}{extra}"
        elif cleaning_steps:
            summary = f"Column: {col} — {len(cleaning_steps)} formatting fix(es) applied"
            likely_cause = f"[{col_type.upper()}] Raw data contained formatting issues"
            suggested_fix = "; ".join(cleaning_steps)
        else:
            summary = f"Column: {col} — no changes needed"
            likely_cause = f"[{col_type.upper()}] Column is already clean"
            suggested_fix = "No action required"

        checks: list[str] = []
        for issue in issues:
            if issue["type"] == "negative_values":
                checks.append(f"Review {issue['count']} negative value(s) — may indicate data entry errors")
            elif issue["type"] == "invalid_email":
                checks.append(f"Validate {issue['count']} malformed email address(es)")
            elif issue["type"] == "mixed_types":
                checks.append("Column contains mixed numeric/text — verify source encoding")

        exp: dict[str, Any] = {
            "anomaly_type": "auto_clean",
            "severity": severity,
            "column": col,
            "raw_message": summary,
            "summary": summary,
            "likely_cause": likely_cause,
            "confidence": "high",
            "recommended_checks": checks,
            "suggested_fix": suggested_fix,
            # Extra fields for rich rendering
            "detected_type": col_type,
            "cleaning_steps": cleaning_steps,
            "issues_found": issues,
            "imputation": imputation,
        }
        explanations.append(exp)

    # Global issues
    for issue in report.get("global_issues", []):
        col = issue.get("column")
        explanations.append({
            "anomaly_type": "global_" + issue["type"],
            "severity": issue["severity"],
            "column": col,
            "raw_message": issue["message"],
            "summary": f"[Dataset] {issue['message']}",
            "likely_cause": "Dataset-level quality issue detected during auto-clean",
            "confidence": "high",
            "recommended_checks": [],
            "suggested_fix": "Inspect flagged rows/columns before further analysis",
        })

    return explanations
