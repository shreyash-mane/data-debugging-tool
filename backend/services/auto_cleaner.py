"""
auto_cleaner.py — Intelligent automatic data cleaning engine.

Handles:
  - Column type detection (numeric, categorical, datetime, text)
  - Symbol/currency stripping and safe numeric conversion
  - Mixed datetime format normalization → always outputs YYYY-MM-DD strings
  - Whitespace trimming for text/categorical
  - Missing value imputation:
      Numeric: skewness-based (mean vs median); drops only if >40% missing AND unimportant
      Categorical: mode for low-cardinality, 'Unknown' for high or high-missing
      Datetime: median date
  - Column importance detection: important columns are NEVER dropped — filled as NaN/Unknown
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

MISSING_DROP_THRESHOLD = 0.40    # drop numeric column if > 40% missing AND unimportant
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

# Keywords that signal a column is semantically important (keep even if >40% missing)
IMPORTANT_COLUMN_KEYWORDS = [
    # identifiers
    "id", "_id", "key", "pk", "uid", "uuid", "ref", "code", "number",
    # names
    "name", "firstname", "first_name", "lastname", "last_name", "fullname",
    "full_name", "title",
    # contact
    "email", "phone", "mobile", "tel", "contact",
    # temporal
    "date", "time", "dob", "birth", "created", "updated", "timestamp", "year",
    # location
    "address", "city", "state", "country", "zip", "postal", "region",
    # demographics
    "age", "gender", "sex",
    # business
    "order", "invoice", "account", "customer", "employee", "user",
]


# ── Column importance scoring ──────────────────────────────────────────────────

def _is_important_column(col_name: str, series: pd.Series) -> tuple[bool, str]:
    """
    Decide whether a column is semantically important enough to preserve
    instead of dropping when it exceeds the missing threshold.

    Returns (is_important, reason_string).
    """
    name_lower = col_name.lower().replace(" ", "_")

    for kw in IMPORTANT_COLUMN_KEYWORDS:
        if kw in name_lower:
            return True, f"column name contains '{kw}'"

    # Near-unique values → likely an ID / natural key
    non_null_count = int(series.notna().sum())
    if non_null_count >= 10:
        unique_ratio = series.nunique(dropna=True) / non_null_count
        if unique_ratio >= 0.9:
            return True, "near-unique values suggest an identifier column"

    return False, "low semantic importance based on name and content"


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

    non_null_mask = series.notna()
    n_stripped = int((str_series[non_null_mask] != stripped[non_null_mask]).sum())
    if n_stripped > 0:
        notes.append(f"Stripped currency/formatting symbols from {n_stripped} value(s)")

    converted = pd.to_numeric(stripped, errors="coerce")

    new_nulls = int(converted.isna().sum()) - int(series.isna().sum())
    if new_nulls > 0:
        notes.append(f"{new_nulls} non-numeric value(s) could not be converted → set to NaN")

    return converted, notes


def _clean_datetime_series(series: pd.Series) -> tuple[pd.Series, list[str]]:
    """
    Normalise mixed datetime formats to datetime64.
    Actual conversion to 'YYYY-MM-DD' strings is done after imputation
    in auto_clean_dataframe so median-date logic still works.
    """
    notes: list[str] = []

    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return series, notes

    converted = pd.to_datetime(series, dayfirst=False, errors="coerce")
    new_nats = int(converted.isna().sum()) - int(series.isna().sum())
    if new_nats > 0:
        notes.append(f"{new_nats} value(s) could not be parsed as dates → set to NaT")
    else:
        notes.append("Date values normalised to consistent internal format")

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
    Auto-impute a numeric column.

    Decision logic:
      - >40% missing AND column is NOT important → "drop_column"
      - >40% missing AND column IS important    → "keep_as_nan" (preserve as-is)
      - ≤40% missing: use skewness to choose mean vs median

    Returns (series, method, reason, stats).
    method: "none" | "mean" | "median" | "drop_column" | "keep_as_nan"
    """
    total = len(series)
    missing = int(series.isna().sum())
    missing_pct = missing / total if total else 0

    if missing_pct > MISSING_DROP_THRESHOLD:
        important, imp_reason = _is_important_column(col_name, series)
        if important:
            return series, "keep_as_nan", (
                f"Missing rate {missing_pct*100:.1f}% exceeds 40%, but column appears "
                f"important ({imp_reason}) — preserved with NaN values rather than dropped"
            ), {
                "missing_count": missing,
                "missing_pct": round(missing_pct * 100, 2),
                "kept_important": True,
            }
        return series, "drop_column", (
            f"Missing rate {missing_pct*100:.1f}% exceeds 40% and column does not appear "
            f"critical — dropped to reduce noise"
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
    """
    Auto-impute a categorical/text column.

    - Low cardinality + low missing → mode
    - High cardinality OR high missing (>40%) → 'Unknown'
    - If column is important with high missing → note that 'Unknown' preserves the column
    """
    total = len(series)
    missing = int(series.isna().sum())
    missing_pct = missing / total if total else 0

    if missing == 0:
        return series, "none", "No missing values", {}

    unique_count = int(series.nunique(dropna=True))

    if missing_pct > HIGH_MISSING_CATEGORICAL or unique_count > LOW_UNIQUE_MAX:
        important, imp_reason = _is_important_column(col_name, series)
        fill_val = "Unknown"
        method = "constant"
        if important:
            reason = (
                f"Column appears important ({imp_reason}); "
                f"high missing rate ({missing_pct*100:.1f}%) → "
                f"filled with 'Unknown' to preserve the column"
            )
        else:
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
    reason = f"Filled {missing} missing date(s) with median date ({str(median_date)[:10]})"
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

    Cleaning decisions:
      Numeric columns:
        - Strip currency/formatting symbols
        - >40% missing AND important → keep with NaN
        - >40% missing AND unimportant → drop
        - ≤40% missing → impute via mean (symmetric) or median (skewed)
      Categorical/text columns:
        - Trim whitespace
        - Low-cardinality + low-missing → mode
        - High-cardinality or high-missing → "Unknown"
      Datetime columns:
        - Normalise all date formats
        - Fill missing with median date
        - Output as "YYYY-MM-DD" strings (consistent format)

    config keys:
      - columns: list[str] | None   → columns to process (None = all)
      - drop_columns_above_threshold: bool  → drop >40% null columns (default True)

    Returns:
      (cleaned_df, report)
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
        elif method == "keep_as_nan":
            # Important column with high missing: keep but do not fill
            col_rep["imputation"] = {
                "decision": "KEEP_AS_NAN",
                "method": "keep_as_nan",
                "reason": reason,
                **stats,
            }
            transformations.append({
                "column": col,
                "action": "keep_as_nan",
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

    # ── Normalise all datetime64 columns to 'YYYY-MM-DD' strings ─────────────
    # This ensures consistent display in sample data, exports, and the UI.
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.Series(
                [v.strftime('%Y-%m-%d') if pd.notna(v) else None for v in df[col]],
                index=df.index,
            )

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

        decision = imputation.get("decision", "") if imputation else ""
        drop = decision == "DROP_COLUMN"
        keep_as_nan = decision == "KEEP_AS_NAN"

        severity = "info"
        if drop:
            severity = "warning"
        elif keep_as_nan:
            severity = "info"
        elif any(i["severity"] == "critical" for i in issues):
            severity = "warning"

        # Build summary
        if drop:
            summary = f"Column '{col}' DROPPED — >{int(MISSING_DROP_THRESHOLD*100)}% missing and not critical"
            likely_cause = imputation.get("reason", "")
            suggested_fix = "Review source data — this column had too many missing values to be useful"
        elif keep_as_nan:
            cnt = imputation.get("missing_count", 0)
            pct = imputation.get("missing_pct", 0)
            summary = f"Column '{col}' preserved with NaN — {cnt} missing ({pct}%)"
            likely_cause = imputation.get("reason", "")
            suggested_fix = "Column kept because it appears important — fill or collect more data"
        elif imputation and decision not in ("", "NONE"):
            m = decision
            cnt = imputation.get("missing_count", 0)
            pct = imputation.get("missing_pct", 0)
            fv = imputation.get("fill_value", "")
            summary = f"Column: {col} — {cnt} missing ({pct}%) filled via {m}"
            likely_cause = f"[{col_type.upper()}] {imputation.get('reason', '')}"
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
