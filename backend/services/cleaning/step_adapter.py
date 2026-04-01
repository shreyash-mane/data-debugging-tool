"""
step_adapter.py — Intelligence enrichment for any pipeline step.

This is the shared backbone that makes the 9-layer cleaning system
available to every step type, not just "Deep Clean".

After a step transforms df_before → df_after, call enrich_step() to:
  - Profile the affected columns
  - Infer their semantic types (age, salary, date, id, …)
  - Detect data quality issues before and after the step
  - Build a per-row audit log of every cell that changed
  - Compute a quality score (0-100) before and after

Column scope:
  - Full-dataset steps (smart_clean, auto_clean, drop_missing, …) → all columns
  - Column-targeted steps (fill_missing, change_dtype, filter_rows, …) → just that column

The returned dict is stored as intelligence_json on the StepSnapshot so every
snapshot in the pipeline carries its own quality score, issue list, and audit trail.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .profiler import profile_dataframe
from .schema_inference import infer_schema
from .issue_detector import detect_issues
from .quality_score import quality_score_pair


# Steps whose scope covers the entire dataframe
_FULL_DATASET_STEPS = frozenset({
    "smart_clean", "auto_clean", "remove_duplicates",
    "group_aggregate", "drop_missing", "join",
})

# ── Public API ─────────────────────────────────────────────────────────────────

def enrich_step(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    step_type: str,
    config: dict,
) -> dict[str, Any]:
    """
    Run intelligence layers on the columns affected by this step.

    Works for every step type — call it after execute_step() returns.

    Returns:
    {
        "affected_columns": [...],
        "issues_before":    [{column, issue_type, detail, examples, severity, ...}, ...],
        "issues_after":     [{...}, ...],
        "issues_fixed":     int,     # how many issues this step resolved
        "audit_log":        [{column, row_index, action, from, to, step_type, layer}, ...],
        "quality_score":    {before_cleaning, after_cleaning, improvement, confidence},
        "schema":           {col: {semantic_type, storage_type, confidence}},
        "layers_run":       [...],
        "errors":           [...],
    }
    """
    affected = _affected_columns(step_type, config, df_before)
    layers_run: list[str] = []
    errors: list[str] = []

    # ── Layer 1: Profiling ──────────────────────────────────────────────────
    try:
        profile_before = profile_dataframe(df_before)
        profile_after  = profile_dataframe(df_after)
        layers_run.append("profiling")
    except Exception as e:
        errors.append(f"profiling: {e}")
        profile_before = profile_after = {"columns": {}, "row_count": 0, "col_count": 0}

    # ── Layer 2: Schema inference ───────────────────────────────────────────
    schema: dict = {}
    full_schema_before: dict = {"per_column": {}}
    full_schema_after:  dict = {"per_column": {}}
    try:
        full_schema_before = infer_schema(df_before, profile_before)
        full_schema_after  = infer_schema(df_after,  profile_after)
        schema = full_schema_before.get("per_column", {})
        layers_run.append("schema_inference")
    except Exception as e:
        errors.append(f"schema_inference: {e}")

    # ── Layer 3: Issue detection (before and after) ─────────────────────────
    issues_before_objs: list = []
    issues_after_objs:  list = []
    try:
        issues_before_objs = detect_issues(df_before, profile_before, full_schema_before)
        layers_run.append("issue_detection_before")
    except Exception as e:
        errors.append(f"issue_detection_before: {e}")

    try:
        issues_after_objs = detect_issues(df_after, profile_after, full_schema_after)
        layers_run.append("issue_detection_after")
    except Exception as e:
        errors.append(f"issue_detection_after: {e}")

    # ── Audit log: cell-level diff on affected columns ──────────────────────
    try:
        audit_log = _build_audit_log(df_before, df_after, affected, step_type)
        layers_run.append("audit_logging")
    except Exception as e:
        errors.append(f"audit_logging: {e}")
        audit_log = []

    # ── Layer 9: Quality scoring ────────────────────────────────────────────
    quality_scores: dict = {}
    try:
        quality_scores = quality_score_pair(
            df_before, df_after, issues_before_objs, issues_after_objs
        )
        layers_run.append("quality_scoring")
    except Exception as e:
        errors.append(f"quality_scoring: {e}")

    issues_before_dicts = [i.to_dict() for i in issues_before_objs]
    issues_after_dicts  = [i.to_dict() for i in issues_after_objs]
    issues_fixed = max(0, len(issues_before_dicts) - len(issues_after_dicts))

    return {
        "affected_columns": affected,
        "issues_before":    issues_before_dicts,
        "issues_after":     issues_after_dicts,
        "issues_fixed":     issues_fixed,
        "audit_log":        audit_log,
        "quality_score":    quality_scores,
        "schema":           schema,
        "layers_run":       layers_run,
        "errors":           errors,
    }


# ── Column scope resolver ──────────────────────────────────────────────────────

def _affected_columns(step_type: str, config: dict, df: pd.DataFrame) -> list[str]:
    """Return the list of columns this step operates on."""
    all_cols = list(df.columns)

    if step_type in _FULL_DATASET_STEPS:
        return all_cols

    if step_type == "fill_missing":
        col = config.get("column")
        return [col] if col and col in df.columns else all_cols

    if step_type in ("change_dtype", "filter_rows"):
        col = config.get("column")
        return [col] if col and col in df.columns else all_cols

    if step_type == "rename_column":
        return [c for c in config.get("mappings", {}).keys() if c in df.columns]

    if step_type == "add_computed_column":
        cols = []
        for key in ("col_a", "col_b"):
            c = config.get(key)
            if c and c in df.columns:
                cols.append(c)
        new_col = config.get("new_column")
        if new_col:
            cols.append(new_col)
        return cols or all_cols

    if step_type == "select_columns":
        return [c for c in config.get("columns", all_cols) if c in df.columns]

    if step_type == "sort_values":
        cols = [c for c in config.get("columns", []) if c in df.columns]
        return cols or all_cols

    return all_cols  # safe default


# ── Audit log builder ──────────────────────────────────────────────────────────

def _build_audit_log(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    affected_cols: list[str],
    step_type: str,
) -> list[dict]:
    """
    Compare df_before and df_after cell-by-cell on affected_cols.
    Returns one audit entry per changed cell.
    """
    audit: list[dict] = []

    common_idx  = df_before.index.intersection(df_after.index)
    cols_shared = [c for c in affected_cols
                   if c in df_before.columns and c in df_after.columns]

    for col in cols_shared:
        b_col = df_before.loc[common_idx, col]
        a_col = df_after.loc[common_idx, col]

        for idx in common_idx:
            bv = b_col[idx]
            av = a_col[idx]

            b_null = _is_null(bv)
            a_null = _is_null(av)

            if b_null and a_null:
                continue
            if not b_null and not a_null and str(bv) == str(av):
                continue

            if b_null and not a_null:
                action = "fill_null"
            elif not b_null and a_null:
                action = "set_null"
            else:
                action = "value_changed"

            audit.append({
                "column":    col,
                "row_index": int(idx),
                "action":    action,
                "from":      _safe_val(bv),
                "to":        _safe_val(av),
                "step_type": step_type,
                "layer":     "step_execution",
            })

    # Row count change entry
    rows_before = len(df_before)
    rows_after  = len(df_after)
    if rows_before != rows_after:
        delta = rows_before - rows_after
        audit.append({
            "column":    "__all__",
            "row_index": None,
            "action":    "rows_changed",
            "from":      rows_before,
            "to":        rows_after,
            "detail":    f"{abs(delta)} row(s) {'dropped' if delta > 0 else 'added'}",
            "step_type": step_type,
            "layer":     "step_execution",
        })

    return audit


# ── Serialisation helpers ──────────────────────────────────────────────────────

def _is_null(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    try:
        return bool(pd.isna(val))
    except Exception:
        return False


def _safe_val(val: Any) -> Any:
    if _is_null(val):
        return None
    if hasattr(val, "item"):          # numpy scalar
        return val.item()
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return val
