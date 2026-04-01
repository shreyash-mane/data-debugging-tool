"""
smart_cleaner.py — Deep intelligent data cleaning engine.

Handles real-world data quality problems in a strict, ordered pipeline:

  Step 1: Replace null-like strings ("not_available", "N/A", "none"…) with NaN
  Step 2: Strip currency symbols (£ $ € ¥ ₹) from numeric-looking columns
  Step 3: Convert English word numbers ("thirty" → 30, "twenty-eight" → 28)
  Step 4: Convert columns to correct types (numeric / datetime)
  Step 5: Remove impossible values (age 0-120, score 0-100, salary > 0)
  Step 6: Fill remaining nulls   (median for numeric, mode for categorical — only if <50% null)
  Step 7: Drop columns that are ≥50% null after all fixes
  Step 8: Remove rows where >50% of columns are null
  Step 9: Remove exact duplicate rows (keep first)

Main API:
  smart_clean_dataframe(df, config) -> (cleaned_df, log)
  analyze_dataset_for_cleaning(df)  -> (suggested_config, issues)
  build_smart_clean_explanations(log, anomalies, diff) -> list[dict]
"""

from __future__ import annotations

import re
from datetime import datetime as _dt
from typing import Any

import numpy as np
import pandas as pd


# ── Constants ──────────────────────────────────────────────────────────────────

NULL_LIKE_SET = frozenset({
    'not_available', 'not available', 'n/a', 'na', 'n.a', 'n.a.',
    'none', 'unknown', 'undefined', 'null', 'nil', 'missing',
    'nan', '#n/a', '#na', '-', '--', '---', 'not applicable',
    'not specified', 'unspecified', 'blank', 'empty',
})

CURRENCY_RE = re.compile(r'[£$€¥₹]')
CURRENCY_STRIP_RE = re.compile(r'[£$€¥₹,\s]')

WORD_TO_NUM: dict[str, int] = {
    'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
    'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
    'eleven': 11, 'twelve': 12, 'thirteen': 13, 'fourteen': 14,
    'fifteen': 15, 'sixteen': 16, 'seventeen': 17, 'eighteen': 18,
    'nineteen': 19, 'twenty': 20, 'thirty': 30, 'forty': 40,
    'fifty': 50, 'sixty': 60, 'seventy': 70, 'eighty': 80, 'ninety': 90,
    'hundred': 100, 'thousand': 1000,
}

# Column name keywords for auto-detection
AGE_KEYWORDS      = ('age', 'years', 'yr', 'yrs', 'years_old')
SCORE_KEYWORDS    = ('score', 'rating', 'grade', 'mark', 'rank', 'points', 'gpa')
SALARY_KEYWORDS   = ('salary', 'wage', 'income', 'pay', 'compensation', 'earnings')
CURRENCY_KEYWORDS = ('salary', 'wage', 'income', 'pay', 'price', 'cost',
                     'revenue', 'profit', 'amount', 'fee', 'rate', 'budget')
DATE_KEYWORDS     = ('date', 'time', 'created', 'updated', 'login', 'signup',
                     'registered', 'modified', 'timestamp', 'at', 'on', 'when', 'dob', 'birth')

EMPTY_ROW_THRESHOLD   = 0.50   # remove rows where >50% cols are null
NULL_DROP_THRESHOLD   = 0.50   # drop columns that are ≥50% null after fixes


# ── Word-number parser ─────────────────────────────────────────────────────────

def _parse_word_number(text: str) -> float | None:
    """
    Parse a simple English word number.  Returns float or None.
    Handles: "thirty", "twenty-eight", "forty five", "nineteen", "one hundred"
    """
    text = text.lower().strip().replace('-', ' ')
    parts = text.split()
    if not parts:
        return None

    if not all(p in WORD_TO_NUM for p in parts):
        return None

    total = 0
    current = 0
    for word in parts:
        val = WORD_TO_NUM[word]
        if val == 1000:
            current = (current if current else 1) * 1000
            total += current
            current = 0
        elif val == 100:
            current = (current if current else 1) * 100
        elif val >= 20:
            current += val
        else:
            current += val

    total += current
    return float(total) if total > 0 else 0.0


def _series_word_number_count(series: pd.Series) -> int:
    """Count how many non-null values in a series look like word numbers."""
    count = 0
    for v in series.dropna().astype(str):
        if _parse_word_number(v) is not None:
            count += 1
    return count


# ── Date helpers ───────────────────────────────────────────────────────────────

# Ordered by specificity — try most specific/unambiguous first
_DATE_FORMATS = [
    '%Y-%m-%d',   # 2023-01-05  ← ISO (already standard)
    '%Y/%m/%d',   # 2023/01/05
    '%d/%m/%Y',   # 15/03/2023  ← UK/EU
    '%m/%d/%Y',   # 04/07/2023  ← US
    '%d-%m-%Y',   # 15-03-2023
    '%m-%d-%Y',   # 04-07-2023
    '%d-%m-%y',   # 15-03-23
    '%m-%d-%y',   # 04-07-23
    '%d %b %Y',   # 15 Mar 2023
    '%b %d, %Y',  # Mar 15, 2023
    '%B %d, %Y',  # March 15, 2023
    '%d %B %Y',   # 15 March 2023
]


def _try_parse(v: str, fmt: str) -> bool:
    try:
        _dt.strptime(v.strip(), fmt)
        return True
    except (ValueError, TypeError):
        return False


def _parse_single_date(val_str: str) -> str | None:
    """
    Try to parse one date string using every known format in order.
    Returns 'YYYY-MM-DD' string on success, None on failure.
    Each value is tried against all formats independently — this prevents
    the "dominant format" problem where minority-format dates stay broken.
    """
    val_str = val_str.strip()
    if not val_str:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return _dt.strptime(val_str, fmt).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            continue
    # Last resort: let pandas infer (handles edge cases like '01-Jan-2023')
    try:
        return pd.to_datetime(val_str, dayfirst=True).strftime('%Y-%m-%d')
    except Exception:
        return None


def _standardize_date_series(series: pd.Series) -> tuple[pd.Series, int, str]:
    """
    Normalise a series of mixed-format dates to 'YYYY-MM-DD' strings.
    Each value is parsed individually against all known formats so that
    columns with mixed formats (e.g. ISO + DD/MM/YYYY + 'Jan 15 2023')
    are fully corrected rather than only the dominant format being fixed.
    Returns (result_series, n_changed, format_description).
    """
    original_str = series.astype(str)
    result_vals: list[str | None] = []

    for val in series:
        if pd.isna(val) or str(val).strip() == '':
            result_vals.append(None)
        else:
            result_vals.append(_parse_single_date(str(val)))

    result = pd.Series(result_vals, index=series.index)

    n_changed = int((result.notna() & (result != original_str)).sum())
    return result, n_changed, 'per-value multi-format'


# ── Step implementations ───────────────────────────────────────────────────────

def _step1_standardize_nulls(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Replace null-like strings with NaN across all object columns."""
    log: dict[str, int] = {}
    df = df.copy()
    for col in df.columns:
        if df[col].dtype != object:
            continue
        mask = df[col].notna() & df[col].astype(str).str.strip().str.lower().isin(NULL_LIKE_SET)
        n = int(mask.sum())
        if n:
            df.loc[mask, col] = np.nan
            log[col] = n
    return df, log


def _step2_strip_currency(df: pd.DataFrame, currency_cols: list[str]) -> tuple[pd.DataFrame, dict]:
    """Strip currency symbols from specified columns and convert to float."""
    log: dict[str, Any] = {}
    df = df.copy()
    for col in currency_cols:
        if col not in df.columns or df[col].dtype != object:
            continue
        non_null = df[col].dropna().astype(str)
        has_sym = non_null.str.contains(CURRENCY_RE, regex=True).sum()
        if has_sym == 0:
            continue
        stripped = df[col].astype(str).str.replace(CURRENCY_STRIP_RE, '', regex=True).str.strip()
        converted = pd.to_numeric(stripped, errors='coerce')
        # Only apply where the original had a currency symbol
        sym_mask = df[col].notna() & df[col].astype(str).str.contains(CURRENCY_RE, regex=True)
        df.loc[sym_mask, col] = converted[sym_mask]
        df[col] = pd.to_numeric(df[col], errors='coerce')
        log[col] = {'symbols_stripped': int(has_sym)}
    return df, log


def _step3_convert_word_numbers(df: pd.DataFrame, numeric_cols: list[str]) -> tuple[pd.DataFrame, dict]:
    """Convert English word numbers to numeric values in specified columns."""
    log: dict[str, int] = {}
    df = df.copy()
    for col in numeric_cols:
        if col not in df.columns:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        n_converted = 0
        new_vals = []
        for v in df[col]:
            if pd.isna(v):
                new_vals.append(v)
            else:
                parsed = _parse_word_number(str(v))
                if parsed is not None:
                    new_vals.append(parsed)
                    n_converted += 1
                else:
                    new_vals.append(v)
        if n_converted > 0:
            df[col] = new_vals
            log[col] = n_converted
    return df, log


def _step4_convert_types(
    df: pd.DataFrame,
    numeric_cols: list[str],
    date_cols: list[str],
) -> tuple[pd.DataFrame, dict]:
    """
    Convert columns to the correct dtype.
    Numeric cols: pd.to_numeric(errors='coerce')
    Date cols: standardize to YYYY-MM-DD strings
    """
    log: dict[str, Any] = {}
    df = df.copy()

    for col in numeric_cols:
        if col not in df.columns or pd.api.types.is_numeric_dtype(df[col]):
            continue
        before_null = int(df[col].isna().sum())
        df[col] = pd.to_numeric(df[col], errors='coerce')
        after_null = int(df[col].isna().sum())
        new_nulls = after_null - before_null
        log[col] = {'type': 'numeric', 'new_nulls_from_coerce': new_nulls}

    for col in date_cols:
        if col not in df.columns:
            continue
        result, n_changed, fmt = _standardize_date_series(df[col].astype(str).where(df[col].notna()))
        df[col] = result
        log[col] = {'type': 'date', 'normalized': n_changed, 'detected_format': fmt}

    return df, log


def _step5_remove_impossible(
    df: pd.DataFrame,
    age_cols: list[str],
    score_cols: list[str],
    salary_cols: list[str],
) -> tuple[pd.DataFrame, dict]:
    """Remove rows with logically impossible values based on column semantics."""
    log: dict[str, Any] = {}
    df = df.copy()
    rows_before = len(df)

    bounds: list[tuple[list[str], float | None, float | None]] = [
        (age_cols,    0.0,  120.0),
        (score_cols,  0.0,  100.0),
        (salary_cols, 0.0,  None),
    ]

    removed_mask = pd.Series(False, index=df.index)
    for cols, lo, hi in bounds:
        for col in cols:
            if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
                continue
            col_mask = pd.Series(False, index=df.index)
            if lo is not None:
                col_mask |= df[col] < lo
            if hi is not None:
                col_mask |= df[col] > hi
            n = int(col_mask.sum())
            if n:
                removed_mask |= col_mask
                log[col] = {'impossible_rows_flagged': n, 'range': f'[{lo}, {hi}]'}

    n_removed = int(removed_mask.sum())
    if n_removed:
        df = df[~removed_mask].reset_index(drop=True)
        log['_total_rows_removed'] = n_removed

    return df, log


def _step6_fill_nulls(df: pd.DataFrame, numeric_cols: list[str]) -> tuple[pd.DataFrame, dict]:
    """
    Fill remaining nulls:
      - Numeric columns: MEDIAN (robust to outliers)
      - Categorical/text columns: MODE; if no mode, 'Unknown'
    Only fill if null% < 50%.  Columns ≥50% null are flagged for dropping (Step 7).
    """
    log: dict[str, Any] = {}
    df = df.copy()

    for col in df.columns:
        null_pct = df[col].isna().mean()
        if null_pct == 0 or null_pct >= NULL_DROP_THRESHOLD:
            continue

        missing = int(df[col].isna().sum())

        if pd.api.types.is_numeric_dtype(df[col]):
            fill_val = float(df[col].median())
            df[col] = df[col].fillna(fill_val)
            log[col] = {
                'method': 'MEDIAN',
                'filled': missing,
                'fill_value': round(fill_val, 4),
                'null_pct': round(null_pct * 100, 1),
            }
        else:
            mode_vals = df[col].mode(dropna=True)
            fill_val_cat = mode_vals[0] if len(mode_vals) > 0 else 'Unknown'
            df[col] = df[col].fillna(fill_val_cat)
            log[col] = {
                'method': 'MODE',
                'filled': missing,
                'fill_value': str(fill_val_cat),
                'null_pct': round(null_pct * 100, 1),
            }

    return df, log


def _step7_drop_high_null_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Drop columns that are still ≥50% null after all prior cleaning."""
    to_drop = [c for c in df.columns if df[c].isna().mean() >= NULL_DROP_THRESHOLD]
    if to_drop:
        df = df.drop(columns=to_drop)
    return df, to_drop


def _step8_remove_empty_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove rows where less than 50% of columns have real data
    (i.e. 50% or more of the columns in that row are null/NaN).
    Uses >= threshold so a row with exactly 50% nulls is also removed.
    Runs before null-filling so we don't impute values into rows that
    should be discarded.
    """
    mask = df.isna().mean(axis=1) >= EMPTY_ROW_THRESHOLD
    n = int(mask.sum())
    if n:
        df = df[~mask].reset_index(drop=True)
    return df, n


def _step9_remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove exact duplicate rows, keeping the first occurrence."""
    n_before = len(df)
    df = df.drop_duplicates(keep='first').reset_index(drop=True)
    return df, n_before - len(df)


# ── Auto-detection ─────────────────────────────────────────────────────────────

def _auto_detect_columns(df: pd.DataFrame) -> dict[str, list[str]]:
    """
    Infer age/score/currency/salary/date column lists from column names and content.
    """
    age_cols: list[str] = []
    score_cols: list[str] = []
    currency_cols: list[str] = []
    salary_cols: list[str] = []
    date_cols: list[str] = []

    for col in df.columns:
        cl = col.lower().replace(' ', '_')
        series = df[col]
        non_null = series.dropna()

        # Age
        if any(kw in cl for kw in AGE_KEYWORDS):
            age_cols.append(col)

        # Score / rating
        if any(kw in cl for kw in SCORE_KEYWORDS):
            score_cols.append(col)

        # Salary columns (for impossible-value check)
        if any(kw in cl for kw in SALARY_KEYWORDS):
            salary_cols.append(col)

        # Currency (name-based OR content-based)
        if any(kw in cl for kw in CURRENCY_KEYWORDS):
            currency_cols.append(col)
        elif series.dtype == object and len(non_null) > 0:
            has_sym = non_null.astype(str).str.contains(CURRENCY_RE, regex=True).mean()
            if has_sym > 0.1:
                currency_cols.append(col)

        # Dates — name hint + content check (try both day-first and month-first)
        name_looks_date = any(kw in cl for kw in DATE_KEYWORDS)
        if series.dtype == object and len(non_null) > 0:
            sample = non_null.astype(str).head(30)
            # Try dayfirst=False (ISO/US) and dayfirst=True (UK/EU) — take best
            parsed_mf = pd.to_datetime(sample, errors='coerce', dayfirst=False)
            parsed_df = pd.to_datetime(sample, errors='coerce', dayfirst=True)
            parse_rate = max(parsed_mf.notna().mean(), parsed_df.notna().mean())
            if parse_rate > 0.4 and (name_looks_date or parse_rate > 0.7):
                date_cols.append(col)

    return {
        'age_columns':      age_cols,
        'score_columns':    score_cols,
        'currency_columns': list(set(currency_cols)),
        'salary_columns':   salary_cols,
        'date_columns':     date_cols,
    }


# ── Main engine ────────────────────────────────────────────────────────────────

def smart_clean_dataframe(
    df: pd.DataFrame,
    config: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Apply all smart-cleaning steps to a DataFrame.

    config keys (all optional — auto-detected if absent):
      age_columns:      list[str]  → apply age bounds validation (0-120)
      score_columns:    list[str]  → apply score bounds validation (0-100)
      currency_columns: list[str]  → strip currency symbols
      salary_columns:   list[str]  → apply salary bounds validation (> 0)
      date_columns:     list[str]  → standardize to YYYY-MM-DD

    Returns (cleaned_df, full_log)
    """
    config = config or {}
    rows_start = len(df)
    cols_start = list(df.columns)

    # ── Auto-detect columns not explicitly provided ────────────────────────────
    detected = _auto_detect_columns(df)

    age_cols      = config.get('age_columns')      or detected['age_columns']
    score_cols    = config.get('score_columns')     or detected['score_columns']
    currency_cols = config.get('currency_columns')  or detected['currency_columns']
    salary_cols   = config.get('salary_columns')    or detected['salary_columns']
    date_cols     = config.get('date_columns')      or detected['date_columns']

    # All columns that should be numeric (for word-number conversion + type coercion)
    numeric_hint_cols = list(set(age_cols + score_cols + currency_cols + salary_cols))

    log: dict[str, Any] = {
        'auto_detected': detected,
        'config_used': {
            'age_columns': age_cols,
            'score_columns': score_cols,
            'currency_columns': currency_cols,
            'salary_columns': salary_cols,
            'date_columns': date_cols,
        },
        'steps': {},
        'summary': {},
    }

    # Step 1: null-like strings → NaN
    df, s1_log = _step1_standardize_nulls(df)
    log['steps']['1_null_standardization'] = s1_log

    # Step 2: strip currency symbols
    df, s2_log = _step2_strip_currency(df, currency_cols)
    log['steps']['2_currency_strip'] = s2_log

    # Step 3: convert word numbers
    df, s3_log = _step3_convert_word_numbers(df, numeric_hint_cols)
    log['steps']['3_word_numbers'] = s3_log

    # Step 4: convert types (numeric + dates)
    df, s4_log = _step4_convert_types(df, numeric_hint_cols, date_cols)
    log['steps']['4_type_conversion'] = s4_log

    # Step 5: remove impossible values
    df, s5_log = _step5_remove_impossible(df, age_cols, score_cols, salary_cols)
    log['steps']['5_impossible_values'] = s5_log

    # Step 6 (moved): remove rows where <50% of columns have real data
    # Must run BEFORE null-filling so we don't impute values into rows
    # that should be discarded entirely.
    df, n_empty_rows = _step8_remove_empty_rows(df)
    log['steps']['6_sparse_rows_removed'] = n_empty_rows

    # Step 7: fill remaining nulls in healthy rows
    df, s6_log = _step6_fill_nulls(df, numeric_hint_cols)
    log['steps']['7_null_fill'] = s6_log

    # Step 8: drop columns still ≥50% null
    df, dropped_cols = _step7_drop_high_null_cols(df)
    log['steps']['8_high_null_cols_dropped'] = dropped_cols

    # Step 9: deduplicate
    df, n_dupes = _step9_remove_duplicates(df)
    log['steps']['9_duplicates_removed'] = n_dupes

    log['summary'] = {
        'rows_before':      rows_start,
        'rows_after':       len(df),
        'rows_removed':     rows_start - len(df),
        'cols_before':      len(cols_start),
        'cols_after':       len(df.columns),
        'cols_dropped':     dropped_cols,
        'nulls_filled':     sum(v.get('filled', 0) for v in s6_log.values() if isinstance(v, dict)),
        'dupes_removed':    n_dupes,
        'sparse_rows_removed': n_empty_rows,
        'word_nums_converted': sum(s3_log.values()),
        'currency_stripped': sum(
            v.get('symbols_stripped', 0) for v in s2_log.values() if isinstance(v, dict)
        ),
        'null_strings_replaced': sum(s1_log.values()),
        'dates_normalized': sum(
            v.get('normalized', 0) for v in s4_log.values()
            if isinstance(v, dict) and v.get('type') == 'date'
        ),
    }

    return df, log


# ── Dataset analysis for suggest-cleaning endpoint ─────────────────────────────

def analyze_dataset_for_cleaning(df: pd.DataFrame) -> tuple[dict, list[dict]]:
    """
    Scan a DataFrame and return (suggested_config, issues_list).

    issues_list entries:
      { column, issue_type, detail, count, severity }
    """
    issues: list[dict] = []
    detected = _auto_detect_columns(df)

    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        n_total = len(series)

        # Null-like strings
        if series.dtype == object and len(non_null) > 0:
            n_null_like = int(
                non_null.astype(str).str.strip().str.lower().isin(NULL_LIKE_SET).sum()
            )
            if n_null_like:
                issues.append({
                    'column': col, 'issue_type': 'null_like_strings',
                    'detail': f'{n_null_like} values look like null (e.g. "not_available", "N/A")',
                    'count': n_null_like,
                    'severity': 'warning' if n_null_like / n_total < 0.3 else 'critical',
                })

        # Currency symbols
        if series.dtype == object and len(non_null) > 0:
            n_curr = int(non_null.astype(str).str.contains(CURRENCY_RE, regex=True).sum())
            if n_curr:
                issues.append({
                    'column': col, 'issue_type': 'currency_symbols',
                    'detail': f'{n_curr} values contain currency symbols (£ $ € …)',
                    'count': n_curr, 'severity': 'warning',
                })

        # Word numbers
        if series.dtype == object and len(non_null) > 0:
            n_word = _series_word_number_count(series)
            if n_word:
                issues.append({
                    'column': col, 'issue_type': 'word_numbers',
                    'detail': f'{n_word} values are written as English words (e.g. "thirty")',
                    'count': n_word, 'severity': 'warning',
                })

        # Missing values
        n_null = int(series.isna().sum())
        if n_null:
            pct = n_null / n_total * 100
            sev = 'critical' if pct >= 50 else 'warning' if pct > 10 else 'info'
            issues.append({
                'column': col, 'issue_type': 'missing_values',
                'detail': f'{n_null} missing values ({pct:.1f}%)',
                'count': n_null, 'severity': sev,
            })

        # Impossible values in known semantic columns
        if col in detected['age_columns'] and pd.api.types.is_numeric_dtype(series):
            bad = int(((series < 0) | (series > 120)).sum())
            if bad:
                issues.append({
                    'column': col, 'issue_type': 'impossible_values',
                    'detail': f'{bad} values outside valid age range (0-120)',
                    'count': bad, 'severity': 'critical',
                })
        if col in detected['score_columns'] and pd.api.types.is_numeric_dtype(series):
            bad = int(((series < 0) | (series > 100)).sum())
            if bad:
                issues.append({
                    'column': col, 'issue_type': 'impossible_values',
                    'detail': f'{bad} values outside valid score range (0-100)',
                    'count': bad, 'severity': 'critical',
                })

        # Mixed date formats
        if col in detected['date_columns'] and series.dtype == object:
            fmt = _detect_best_date_format(series)
            if fmt and fmt != '%Y-%m-%d':
                sample = non_null.astype(str).str.strip().head(50)
                n_diff = int(sample[~sample.str.match(r'^\d{4}-\d{2}-\d{2}$')].count())
                if n_diff:
                    issues.append({
                        'column': col, 'issue_type': 'mixed_date_formats',
                        'detail': f'{n_diff} values are not in YYYY-MM-DD format (detected: {fmt})',
                        'count': n_diff, 'severity': 'warning',
                    })

    # Dataset-level issues
    dup_count = int(df.duplicated().sum())
    if dup_count:
        issues.append({
            'column': None, 'issue_type': 'duplicate_rows',
            'detail': f'{dup_count} exact duplicate rows',
            'count': dup_count,
            'severity': 'critical' if dup_count / len(df) > 0.1 else 'warning',
        })

    empty_rows = int((df.isna().mean(axis=1) > EMPTY_ROW_THRESHOLD).sum())
    if empty_rows:
        issues.append({
            'column': None, 'issue_type': 'mostly_empty_rows',
            'detail': f'{empty_rows} rows where >50% of columns are empty',
            'count': empty_rows, 'severity': 'critical',
        })

    suggested_config = {
        'age_columns':      detected['age_columns'],
        'score_columns':    detected['score_columns'],
        'currency_columns': detected['currency_columns'],
        'salary_columns':   detected['salary_columns'],
        'date_columns':     detected['date_columns'],
    }
    return suggested_config, issues


# ── Explanation builder ────────────────────────────────────────────────────────

def build_smart_clean_explanations(
    log: dict,
    anomalies: list[dict],
    diff: dict,
) -> list[dict]:
    """Convert a smart_clean log into ExplanationPanel-compatible dicts."""
    explanations: list[dict] = []
    summary = log.get('summary', {})
    steps = log.get('steps', {})

    # Per-column explanations from null standardization
    for col, count in (steps.get('1_null_standardization') or {}).items():
        explanations.append({
            'anomaly_type': 'smart_clean',
            'severity': 'info',
            'column': col,
            'summary': f"Column '{col}': {count} null-like string(s) replaced with NaN",
            'raw_message': f"{count} values like 'not_available', 'N/A' etc. → NaN",
            'likely_cause': 'Source data used text placeholders instead of actual nulls',
            'confidence': 'high',
            'recommended_checks': [],
            'suggested_fix': 'Standardized — null-like strings replaced with proper NaN',
        })

    # Currency stripping
    for col, info in (steps.get('2_currency_strip') or {}).items():
        if isinstance(info, dict):
            n = info.get('symbols_stripped', 0)
            explanations.append({
                'anomaly_type': 'smart_clean',
                'severity': 'info',
                'column': col,
                'summary': f"Column '{col}': {n} currency symbol(s) stripped",
                'raw_message': f"Removed £ $ € symbols from {n} values and converted to numeric",
                'likely_cause': 'Currency data stored as formatted strings (e.g. "£45000")',
                'confidence': 'high',
                'recommended_checks': [],
                'suggested_fix': 'Stripped and converted to numeric',
            })

    # Word number conversion
    for col, count in (steps.get('3_word_numbers') or {}).items():
        explanations.append({
            'anomaly_type': 'smart_clean',
            'severity': 'warning',
            'column': col,
            'summary': f"Column '{col}': {count} word number(s) converted to numeric",
            'raw_message': f"{count} values like 'thirty', 'twenty-eight' converted to digits",
            'likely_cause': 'Data entry inconsistency — some values entered as words',
            'confidence': 'high',
            'recommended_checks': ['Verify converted values are correct'],
            'suggested_fix': 'Converted English word numbers to numeric',
        })

    # Date normalization
    for col, info in (steps.get('4_type_conversion') or {}).items():
        if isinstance(info, dict) and info.get('type') == 'date':
            n = info.get('normalized', 0)
            fmt = info.get('detected_format', 'unknown')
            if n:
                explanations.append({
                    'anomaly_type': 'smart_clean',
                    'severity': 'warning',
                    'column': col,
                    'summary': f"Column '{col}': {n} date(s) standardized to YYYY-MM-DD",
                    'raw_message': f"Detected format '{fmt}', normalized {n} values to ISO YYYY-MM-DD",
                    'likely_cause': 'Mixed date formats in source data',
                    'confidence': 'high',
                    'recommended_checks': [],
                    'suggested_fix': 'All dates now in YYYY-MM-DD format',
                })

    # Impossible values
    imp = steps.get('5_impossible_values') or {}
    for col, info in imp.items():
        if col == '_total_rows_removed' or not isinstance(info, dict):
            continue
        n = info.get('impossible_rows_flagged', 0)
        rng = info.get('range', '')
        explanations.append({
            'anomaly_type': 'smart_clean',
            'severity': 'critical',
            'column': col,
            'summary': f"Column '{col}': {n} row(s) removed — value outside valid range {rng}",
            'raw_message': f"{n} impossible values (outside {rng}) removed",
            'likely_cause': 'Data entry error or corrupted records',
            'confidence': 'high',
            'recommended_checks': [f'Investigate why values outside {rng} existed'],
            'suggested_fix': f'Rows with impossible {col} values removed',
        })

    # Null filling
    for col, info in (steps.get('6_null_fill') or {}).items():
        if not isinstance(info, dict):
            continue
        method = info.get('method', '')
        filled = info.get('filled', 0)
        fv = info.get('fill_value', '')
        pct = info.get('null_pct', 0)
        explanations.append({
            'anomaly_type': 'smart_clean',
            'severity': 'info',
            'column': col,
            'summary': f"Column '{col}': {filled} null(s) filled via {method} ({pct}% missing)",
            'raw_message': f"Filled {filled} nulls with {method} = {fv}",
            'likely_cause': f'Column had {pct}% missing values',
            'confidence': 'high',
            'recommended_checks': [],
            'suggested_fix': f'Used {method} ({"robust to outliers" if method == "MEDIAN" else "most frequent value"})',
        })

    # Dropped columns
    dropped = steps.get('7_high_null_cols_dropped') or []
    for col in dropped:
        explanations.append({
            'anomaly_type': 'smart_clean',
            'severity': 'warning',
            'column': col,
            'summary': f"Column '{col}' dropped — ≥50% null after all cleaning",
            'raw_message': 'Column exceeded null threshold and was removed',
            'likely_cause': 'Column has insufficient data to be useful',
            'confidence': 'high',
            'recommended_checks': ['Review source data collection for this column'],
            'suggested_fix': 'Column removed to reduce noise',
        })

    # Dataset-level summary
    n_rows_removed = summary.get('rows_removed', 0)
    n_dupes = summary.get('dupes_removed', 0)
    n_empty = summary.get('empty_rows_removed', 0)
    if n_rows_removed > 0:
        explanations.append({
            'anomaly_type': 'smart_clean',
            'severity': 'warning',
            'column': None,
            'summary': (
                f"[Dataset] {n_rows_removed} row(s) removed total: "
                f"{n_dupes} duplicate(s), {n_empty} mostly-empty"
            ),
            'raw_message': f"Removed {n_dupes} duplicate rows and {n_empty} rows with >50% empty cols",
            'likely_cause': 'Duplicate entries or records with insufficient data',
            'confidence': 'high',
            'recommended_checks': [],
            'suggested_fix': 'Duplicates removed (kept first); near-empty rows dropped',
        })

    return explanations
