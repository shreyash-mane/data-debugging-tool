"""
Cleaning Pipeline (Orchestrator)
=================================
Runs all 9 layers in sequence and assembles the final API response.

Usage
-----
    from services.cleaning.cleaning_pipeline import run_pipeline

    result = run_pipeline(df, config={"duplicate_keep": "first"})

Designed to be called from FastAPI routes (or any other caller).
The pipeline is stateless — each call creates fresh state.
"""

from __future__ import annotations

import traceback
from typing import Any

import pandas as pd

from .profiler import profile_dataframe
from .schema_inference import infer_schema
from .issue_detector import detect_issues
from .normalizer import normalize
from .validator import validate
from .repair import repair
from .duplicates import handle_duplicates, DuplicateConfig
from .audit import summarise_audit
from .quality_score import quality_score_pair


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_pipeline(
    df: pd.DataFrame,
    config: dict[str, Any] | None = None,
    layers: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run the full cleaning pipeline on *df*.

    Parameters
    ----------
    df:
        Raw input dataframe (not modified — a copy is made internally).
    config:
        Optional overrides:
          - duplicate_keep: "first" | "last" | "none"
          - validation_rules: dict passed to validator
    layers:
        Subset of layers to run (default: all).  Useful for phased rollout.

    Returns
    -------
    dict matching the documented API response shape.
    """
    config = config or {}
    all_layers = [
        "profiling",
        "schema_inference",
        "issue_detection",
        "normalization",
        "validation",
        "repair",
        "duplicate_handling",
        "audit_logging",
        "quality_scoring",
    ]
    layers_to_run = set(layers or all_layers)

    audit_log: list[dict] = []
    errors: list[str] = []
    layers_run: list[str] = []

    # ── Snapshot original ───────────────────────────────────────────────────
    df_original = df.copy()

    # ── Layer 1: Profiling ──────────────────────────────────────────────────
    profile = {}
    if "profiling" in layers_to_run:
        try:
            profile = profile_dataframe(df_original)
            layers_run.append("profiling")
        except Exception as e:
            errors.append(f"profiling: {e}")

    # ── Layer 2: Schema Inference ───────────────────────────────────────────
    schema: dict[str, Any] = {"per_column": {}}
    if "schema_inference" in layers_to_run and profile:
        try:
            schema = infer_schema(df_original, profile)
            layers_run.append("schema_inference")
        except Exception as e:
            errors.append(f"schema_inference: {e}")

    # ── Layer 3: Issue Detection (pre-clean) ────────────────────────────────
    issues_before = []
    if "issue_detection" in layers_to_run and profile:
        try:
            issues_before = detect_issues(df_original, profile, schema)
            layers_run.append("issue_detection")
        except Exception as e:
            errors.append(f"issue_detection: {e}")

    # ── Layer 4: Normalization ──────────────────────────────────────────────
    df_work = df_original.copy()
    if "normalization" in layers_to_run and schema:
        try:
            df_work = normalize(df_work, schema, audit_log)
            layers_run.append("normalization")
        except Exception as e:
            errors.append(f"normalization: {e}")
            traceback.print_exc()

    # ── Layer 4.5: Drop empty rows BEFORE repair so we don't impute into them ─
    if "duplicate_handling" in layers_to_run:
        try:
            df_work = _drop_empty_rows_early(df_work, schema, audit_log)
        except Exception as e:
            errors.append(f"empty_row_removal: {e}")

    # ── Layer 5: Validation ─────────────────────────────────────────────────
    if "validation" in layers_to_run and schema:
        try:
            validation_rules = config.get("validation_rules", {})
            df_work = validate(df_work, schema, audit_log, validation_rules)
            layers_run.append("validation")
        except Exception as e:
            errors.append(f"validation: {e}")

    # ── Layer 6: Repair / Imputation ────────────────────────────────────────
    if "repair" in layers_to_run and profile and schema:
        try:
            df_work = repair(df_work, schema, profile, audit_log)
            layers_run.append("repair")
        except Exception as e:
            errors.append(f"repair: {e}")

    # ── Layer 7: Duplicate Handling (dedup only — empty rows already dropped) ─
    if "duplicate_handling" in layers_to_run and schema:
        try:
            dup_config = DuplicateConfig(
                keep=config.get("duplicate_keep", "first"),
                id_columns=schema.get("id_columns", []),
            )
            df_work = handle_duplicates(df_work, schema, audit_log, dup_config)
            layers_run.append("duplicate_handling")
        except Exception as e:
            errors.append(f"duplicate_handling: {e}")

    # ── Layer 8: Audit Logging ──────────────────────────────────────────────
    audit_summary = {}
    if "audit_logging" in layers_to_run:
        try:
            audit_summary = summarise_audit(audit_log)
            layers_run.append("audit_logging")
        except Exception as e:
            errors.append(f"audit_logging: {e}")

    # ── Layer 9: Quality Scoring ────────────────────────────────────────────
    quality_scores: dict[str, Any] = {}
    if "quality_scoring" in layers_to_run and issues_before is not None:
        try:
            # Re-detect issues on cleaned data for comparison
            if profile:
                profile_after = profile_dataframe(df_work)
                schema_after = infer_schema(df_work, profile_after)
                issues_after = detect_issues(df_work, profile_after, schema_after)
            else:
                issues_after = []

            quality_scores = quality_score_pair(
                df_original, df_work, issues_before, issues_after
            )
            layers_run.append("quality_scoring")
        except Exception as e:
            errors.append(f"quality_scoring: {e}")

    # ── Build issue summary ─────────────────────────────────────────────────
    issue_dicts = [i.to_dict() for i in issues_before]
    severity_counts = {"critical": 0, "warning": 0, "info": 0}
    for i in issues_before:
        severity_counts[i.severity] = severity_counts.get(i.severity, 0) + 1

    # ── Assemble response ───────────────────────────────────────────────────
    suggested_config = {k: v for k, v in schema.items() if k != "per_column"}

    return {
        "layers_run": layers_run,
        "suggested_config": suggested_config,
        "schema_per_column": schema.get("per_column", {}),
        "issues": issue_dicts,
        "summary": {
            "total_issues": len(issue_dicts),
            "critical": severity_counts.get("critical", 0),
            "warning": severity_counts.get("warning", 0),
            "info": severity_counts.get("info", 0),
        },
        "quality_score": quality_scores,
        "audit_log": audit_log,
        "audit_summary": audit_summary,
        "cleaned_preview": _df_to_preview(df_work, max_rows=50),
        "row_count_before": len(df_original),
        "row_count_after": len(df_work),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _df_to_preview(df: pd.DataFrame, max_rows: int = 50) -> list[dict]:
    """Convert dataframe head to JSON-serialisable list of dicts."""
    preview = df.head(max_rows).copy()
    # Replace NaN with None for JSON
    preview = preview.where(pd.notnull(preview), None)
    return preview.to_dict(orient="records")


def _drop_empty_rows_early(
    df: pd.DataFrame,
    schema: dict[str, Any],
    audit_log: list[dict],
) -> pd.DataFrame:
    """
    Remove rows where all data columns (non-ID) are null/empty.
    Called before repair so we never impute into empty rows.
    """
    import re as _re
    check = df.replace(r"^\s*$", pd.NA, regex=True)
    id_like = [c for c in df.columns if _re.search(r"\bid\b|_id$|^id_", c.lower())]
    data_cols = [c for c in df.columns if c not in id_like]
    col_set = data_cols if data_cols else list(df.columns)

    all_null_mask = check[col_set].isnull().all(axis=1)
    n = int(all_null_mask.sum())
    if n == 0:
        return df

    empty_indices = df[all_null_mask].index.tolist()
    df = df[~all_null_mask].copy()

    audit_log.append({
        "column": "__all__",
        "row_index": empty_indices,
        "action": "drop_empty_rows",
        "from": f"{n} empty row(s)",
        "to": "dropped",
        "detail": (
            f"Removed {n} row(s) where all data columns were null/empty "
            f"(original row indices: {empty_indices})."
        ),
        "confidence": "high",
        "layer": "duplicate_handling",
    })
    return df
