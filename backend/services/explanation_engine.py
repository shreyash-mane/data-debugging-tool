"""
explanation_engine.py — Generates human-readable root cause explanations
from anomalies and diff data.  No external API required — rule-based.
"""

from __future__ import annotations


# Severity ranking for sorting
_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}


def generate_explanations(
    anomalies: list[dict],
    diff: dict,
    step_type: str,
    step_name: str,
) -> list[dict]:
    """
    For each anomaly, generate a structured explanation dict:
      {
        anomaly_type, severity, column,
        likely_cause, confidence,       # "high" / "medium" / "low"
        recommended_checks,             # list of strings
        suggested_fix,                  # string
        summary,                        # one-liner for the card title
      }
    """
    explanations: list[dict] = []

    for a in sorted(anomalies, key=lambda x: _SEVERITY_RANK.get(x["severity"], 3)):
        exp = _explain_anomaly(a, diff, step_type, step_name)
        if exp:
            explanations.append(exp)

    return explanations


def _explain_anomaly(a: dict, diff: dict, step_type: str, step_name: str) -> dict | None:
    atype = a["type"]
    col = a.get("column")
    val = a.get("value")

    base = {
        "anomaly_type": atype,
        "severity": a["severity"],
        "column": col,
        "raw_message": a["message"],
    }

    # ── Large row drop ────────────────────────────────────────────────────────
    if atype == "large_row_drop":
        pct = abs(val) if val is not None else "?"
        if step_type == "filter_rows":
            cause = f"The filter condition removed {pct:.1f}% of rows."
            fix = "Review the filter expression — check for off-by-one errors, wrong comparison operators, or unexpected null values in the filter column."
            confidence = "high"
            checks = [
                "Print value counts of the filter column before this step.",
                "Check if the filter column contains nulls that are being excluded.",
                "Verify the filter operator (e.g., > vs >=, == vs !=).",
            ]
        elif step_type in ("join", "merge"):
            cause = f"The join on the specified key(s) found only {100 - pct:.1f}% matching rows."
            fix = "Inspect key columns on both sides for mismatches — check for whitespace, case differences, or type mismatches."
            confidence = "high"
            checks = [
                "Compare unique key values in both datasets.",
                "Check for leading/trailing whitespace in key columns.",
                "Ensure key column data types match on both sides.",
            ]
        elif step_type == "drop_missing":
            cause = "Many rows had null values in one or more columns, causing them to be dropped."
            fix = "Check null counts per column before this step. Consider using fill_missing instead of drop_missing for non-critical columns."
            confidence = "high"
            checks = [
                "Review which columns have the most nulls.",
                "Determine if dropping nulls is intentional for all columns.",
            ]
        else:
            cause = f"The transformation at step '{step_name}' significantly reduced the dataset."
            fix = "Inspect the step configuration for overly aggressive parameters."
            confidence = "medium"
            checks = ["Review the step configuration.", "Compare sample data before and after."]

        return {
            **base,
            "summary": f"Row count dropped {pct:.1f}%",
            "likely_cause": cause,
            "confidence": confidence,
            "recommended_checks": checks,
            "suggested_fix": fix,
        }

    # ── Row explosion ─────────────────────────────────────────────────────────
    if atype == "row_explosion":
        return {
            **base,
            "summary": f"Row count exploded +{val:.1f}%",
            "likely_cause": "The join likely produced a many-to-many match (cartesian product). Non-unique keys on one or both sides multiply rows.",
            "confidence": "high",
            "recommended_checks": [
                "Check if the join key is unique in both datasets.",
                "Use value_counts() on the join key before joining.",
                "Consider deduplicating keys before joining.",
            ],
            "suggested_fix": "Add a deduplication step before the join, or use a left/right join if appropriate.",
        }

    # ── Column mostly null ────────────────────────────────────────────────────
    if atype == "column_mostly_null":
        if step_type == "change_dtype":
            cause = f"Type conversion of '{col}' introduced nulls — values that couldn't be converted became NaN."
            fix = f"Inspect the raw values in '{col}' before conversion. Clean or replace non-parseable values first."
            confidence = "high"
        elif step_type in ("join", "merge"):
            cause = f"Column '{col}' came from the right dataset and most rows didn't match the join key."
            fix = "Check key alignment between datasets. Consider filling nulls after join."
            confidence = "high"
        else:
            cause = f"Column '{col}' became mostly null after step '{step_name}'. The transformation may have overwritten or invalidated values."
            fix = f"Review the step logic for column '{col}'."
            confidence = "medium"
        return {
            **base,
            "summary": f"'{col}' became mostly null ({val:.1f}%)",
            "likely_cause": cause,
            "confidence": confidence,
            "recommended_checks": [
                f"Run value_counts() on '{col}' before this step.",
                "Check if any intermediate steps silently drop or nullify values.",
            ],
            "suggested_fix": fix,
        }

    # ── Null increase ─────────────────────────────────────────────────────────
    if atype == "null_increase":
        return {
            **base,
            "summary": f"'{col}' gained nulls ({val:.1f}%)",
            "likely_cause": f"Column '{col}' acquired new null values. Common causes: type coercion failures, join mismatches, or computed column errors.",
            "confidence": "medium",
            "recommended_checks": [
                f"Check the distribution of '{col}' before this step.",
                "Look for values that couldn't be parsed or computed.",
            ],
            "suggested_fix": f"Add a fill_missing step after this to handle nulls in '{col}', or investigate the source of null values.",
        }

    # ── Type change ───────────────────────────────────────────────────────────
    if atype == "type_change":
        return {
            **base,
            "summary": f"'{col}' type changed: {val}",
            "likely_cause": f"The data type of '{col}' changed, which may cause downstream issues.",
            "confidence": "low",
            "recommended_checks": [
                f"Verify that all downstream steps expect the new type of '{col}'.",
                "Check if any values were lost or coerced incorrectly.",
            ],
            "suggested_fix": "If unintentional, add an explicit change_dtype step to restore the original type.",
        }

    # ── Duplicate spike ────────────────────────────────────────────────────────
    if atype in ("duplicate_spike", "duplicates_appeared"):
        return {
            **base,
            "summary": f"Duplicates {'spiked' if atype == 'duplicate_spike' else 'appeared'} ({val}{'%' if atype == 'duplicate_spike' else ' rows'})",
            "likely_cause": "Duplicate rows often appear after a join with non-unique keys or after an aggregation that doesn't fully collapse rows.",
            "confidence": "medium",
            "recommended_checks": [
                "Check if join keys are unique in both datasets.",
                "Run a deduplicate step and see if it changes row count significantly.",
            ],
            "suggested_fix": "Add a remove_duplicates step after this transformation to clean the data.",
        }

    # ── Stat drift ────────────────────────────────────────────────────────────
    if atype == "stat_drift":
        return {
            **base,
            "summary": f"'{col}' mean drifted {val:+.1f}%",
            "likely_cause": f"The mean of '{col}' shifted significantly. This may be expected (e.g., after filtering a specific segment) or indicate data corruption.",
            "confidence": "medium",
            "recommended_checks": [
                f"Plot or describe '{col}' before and after to visualise the change.",
                "Verify the transformation is not inadvertently removing a systematic segment.",
            ],
            "suggested_fix": "If unexpected, inspect which rows were removed and whether they were biased toward higher or lower values.",
        }

    # ── Distribution shift ─────────────────────────────────────────────────────
    if atype == "distribution_shift":
        return {
            **base,
            "summary": f"'{col}' distribution shifted (KS={val:.3f})",
            "likely_cause": f"The statistical distribution of '{col}' changed significantly between steps. Values are now spread differently.",
            "confidence": "medium",
            "recommended_checks": [
                f"Compare histograms of '{col}' before and after.",
                "Check if the step inadvertently resampled or transformed numeric values.",
            ],
            "suggested_fix": "Investigate whether the distribution change is intentional or caused by an incorrect transformation.",
        }

    # ── Category disappear ────────────────────────────────────────────────────
    if atype == "category_disappear":
        return {
            **base,
            "summary": f"'{col}' lost {val} category values",
            "likely_cause": f"Some category values in '{col}' were filtered out or not present in joined data.",
            "confidence": "medium",
            "recommended_checks": [
                f"Run value_counts() on '{col}' before and after.",
                "Determine if the missing categories are expected to disappear.",
            ],
            "suggested_fix": "If the disappearance is unintentional, review the filter or join condition to ensure all categories are retained.",
        }

    return None
