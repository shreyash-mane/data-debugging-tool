"""
diff_engine.py — Compare two DataFrames (consecutive pipeline steps).
Returns a structured diff dict that feeds both the UI and anomaly detector.
"""

import json
import math
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats


def _safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


def compute_diff(before: pd.DataFrame, after: pd.DataFrame) -> dict:
    """
    Compare two DataFrames and return a structured diff dictionary.

    Keys in the returned dict:
    - row_count_before / row_count_after / row_delta / row_delta_pct
    - col_count_before / col_count_after
    - columns_added / columns_removed / columns_renamed (heuristic)
    - type_changes: {col: {before, after}}
    - null_changes: {col: {before, after, delta, delta_pct}}
    - duplicate_before / duplicate_after / duplicate_delta
    - stat_drift: {col: {mean_before, mean_after, drift_pct}}
    - category_shifts: {col: {added_values, removed_values}}
    - distribution_shift: {col: {ks_stat, ks_pvalue}}  (numeric only)
    """
    diff: dict = {}

    # ── Row counts ──────────────────────────────────────────────────────────
    rb, ra = len(before), len(after)
    diff["row_count_before"] = rb
    diff["row_count_after"] = ra
    diff["row_delta"] = ra - rb
    diff["row_delta_pct"] = _safe(round((ra - rb) / rb * 100, 2)) if rb else None

    # ── Column counts ────────────────────────────────────────────────────────
    cols_before = set(before.columns)
    cols_after = set(after.columns)
    diff["col_count_before"] = len(cols_before)
    diff["col_count_after"] = len(cols_after)
    diff["columns_added"] = list(cols_after - cols_before)
    diff["columns_removed"] = list(cols_before - cols_after)

    # ── Type changes (for columns present in both) ────────────────────────
    common_cols = cols_before & cols_after
    type_changes = {}
    for col in common_cols:
        tb = str(before[col].dtype)
        ta = str(after[col].dtype)
        if tb != ta:
            type_changes[col] = {"before": tb, "after": ta}
    diff["type_changes"] = type_changes

    # ── Null changes ─────────────────────────────────────────────────────────
    null_changes = {}
    for col in common_cols:
        nb = int(before[col].isna().sum())
        na = int(after[col].isna().sum())
        delta = na - nb
        pct = _safe(round(delta / rb * 100, 2)) if rb else None
        null_changes[col] = {
            "before": nb,
            "after": na,
            "delta": delta,
            "delta_pct": pct,
        }
    # Also add new columns null info
    for col in cols_after - cols_before:
        na = int(after[col].isna().sum())
        null_changes[col] = {"before": None, "after": na, "delta": None, "delta_pct": None}
    diff["null_changes"] = null_changes

    # ── Duplicates ───────────────────────────────────────────────────────────
    db = int(before.duplicated().sum())
    da = int(after.duplicated().sum())
    diff["duplicate_before"] = db
    diff["duplicate_after"] = da
    diff["duplicate_delta"] = da - db

    # ── Numeric stat drift ───────────────────────────────────────────────────
    stat_drift = {}
    for col in common_cols:
        if pd.api.types.is_numeric_dtype(before[col]) and pd.api.types.is_numeric_dtype(after[col]):
            mb = _safe(before[col].mean())
            ma = _safe(after[col].mean())
            if mb is not None and ma is not None and mb != 0:
                drift_pct = _safe(round((ma - mb) / abs(mb) * 100, 2))
            else:
                drift_pct = None
            stat_drift[col] = {
                "mean_before": mb,
                "mean_after": ma,
                "drift_pct": drift_pct,
                "min_before": _safe(before[col].min()),
                "min_after": _safe(after[col].min()),
                "max_before": _safe(before[col].max()),
                "max_after": _safe(after[col].max()),
            }
    diff["stat_drift"] = stat_drift

    # ── Category shifts ───────────────────────────────────────────────────────
    category_shifts = {}
    for col in common_cols:
        if not pd.api.types.is_numeric_dtype(before[col]):
            vb = set(before[col].dropna().unique())
            va = set(after[col].dropna().unique())
            added = list(va - vb)
            removed = list(vb - va)
            if added or removed:
                category_shifts[col] = {
                    "added_values": [str(v) for v in added[:20]],
                    "removed_values": [str(v) for v in removed[:20]],
                }
    diff["category_shifts"] = category_shifts

    # ── KS distribution shift (numeric, sampled for performance) ─────────────
    distribution_shift = {}
    for col in common_cols:
        if pd.api.types.is_numeric_dtype(before[col]) and pd.api.types.is_numeric_dtype(after[col]):
            b_vals = before[col].dropna().sample(min(500, len(before[col].dropna())), random_state=42) \
                if len(before[col].dropna()) > 0 else pd.Series([], dtype=float)
            a_vals = after[col].dropna().sample(min(500, len(after[col].dropna())), random_state=42) \
                if len(after[col].dropna()) > 0 else pd.Series([], dtype=float)
            if len(b_vals) > 1 and len(a_vals) > 1:
                try:
                    ks_stat, ks_p = scipy_stats.ks_2samp(b_vals.values, a_vals.values)
                    distribution_shift[col] = {
                        "ks_stat": _safe(round(float(ks_stat), 4)),
                        "ks_pvalue": _safe(round(float(ks_p), 4)),
                    }
                except Exception:
                    pass
    diff["distribution_shift"] = distribution_shift

    return diff
