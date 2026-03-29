"""
anomaly_detector.py — Heuristic-based anomaly detection on step diffs.

Configurable thresholds allow tuning sensitivity without code changes.
Each anomaly is a dict with: type, severity, column (optional), message, value.
"""

from __future__ import annotations

# ── Default thresholds (can be overridden per pipeline in future) ─────────────
THRESHOLDS = {
    "row_drop_pct": 30,          # % row drop to flag as warning
    "row_drop_pct_critical": 70, # % row drop to flag as critical
    "row_increase_pct": 50,      # % row increase (join explosion)
    "null_increase_pct": 20,     # absolute % of total rows turning null
    "null_mostly_threshold": 80, # column becomes X% null → critical
    "duplicate_spike_pct": 50,   # % increase in duplicates
    "stat_drift_pct": 50,        # % mean shift to flag
    "ks_stat_threshold": 0.3,    # KS stat above this → distribution shift
    "ks_pvalue_threshold": 0.05, # KS p-value below this → significant
    "category_disappear_ratio": 0.3,  # fraction of categories removed
}


def detect_anomalies(diff: dict, step_name: str, row_count_before: int) -> list[dict]:
    """
    Analyse a diff dict and return a list of anomaly dicts.

    Each anomaly dict:
      {type, severity, column, message, value, threshold}
    """
    anomalies: list[dict] = []
    t = THRESHOLDS

    # ── Row count changes ─────────────────────────────────────────────────────
    row_delta_pct = diff.get("row_delta_pct")
    if row_delta_pct is not None:
        if row_delta_pct <= -t["row_drop_pct_critical"]:
            anomalies.append({
                "type": "large_row_drop",
                "severity": "critical",
                "column": None,
                "message": (
                    f"Row count dropped by {abs(row_delta_pct):.1f}% in step '{step_name}'. "
                    "This is likely an aggressive filter or a bad join condition."
                ),
                "value": row_delta_pct,
                "threshold": -t["row_drop_pct_critical"],
            })
        elif row_delta_pct <= -t["row_drop_pct"]:
            anomalies.append({
                "type": "large_row_drop",
                "severity": "warning",
                "column": None,
                "message": (
                    f"Row count dropped by {abs(row_delta_pct):.1f}% in step '{step_name}'. "
                    "Review the filter or join condition."
                ),
                "value": row_delta_pct,
                "threshold": -t["row_drop_pct"],
            })

        if row_delta_pct >= t["row_increase_pct"]:
            anomalies.append({
                "type": "row_explosion",
                "severity": "warning",
                "column": None,
                "message": (
                    f"Row count increased by {row_delta_pct:.1f}% in step '{step_name}'. "
                    "This may indicate a cartesian join or duplicate keys."
                ),
                "value": row_delta_pct,
                "threshold": t["row_increase_pct"],
            })

    # ── Null changes ──────────────────────────────────────────────────────────
    null_changes = diff.get("null_changes", {})
    row_after = diff.get("row_count_after", 1) or 1

    for col, nc in null_changes.items():
        after_nulls = nc.get("after", 0) or 0
        before_nulls = nc.get("before", 0) or 0
        delta = nc.get("delta", 0) or 0

        null_pct_after = after_nulls / row_after * 100
        null_pct_increase = delta / row_count_before * 100 if row_count_before else 0

        if null_pct_after >= t["null_mostly_threshold"] and after_nulls > before_nulls:
            anomalies.append({
                "type": "column_mostly_null",
                "severity": "critical",
                "column": col,
                "message": (
                    f"Column '{col}' is now {null_pct_after:.1f}% null after step '{step_name}'. "
                    "Likely caused by a failed type conversion, bad merge key, or aggressive filter."
                ),
                "value": null_pct_after,
                "threshold": t["null_mostly_threshold"],
            })
        elif null_pct_increase >= t["null_increase_pct"] and delta > 0:
            anomalies.append({
                "type": "null_increase",
                "severity": "warning",
                "column": col,
                "message": (
                    f"Column '{col}' gained {delta} new nulls ({null_pct_increase:.1f}% of rows) "
                    f"in step '{step_name}'."
                ),
                "value": null_pct_increase,
                "threshold": t["null_increase_pct"],
            })

    # ── Type changes ──────────────────────────────────────────────────────────
    for col, tc in diff.get("type_changes", {}).items():
        anomalies.append({
            "type": "type_change",
            "severity": "info",
            "column": col,
            "message": (
                f"Column '{col}' changed type from {tc['before']} → {tc['after']} "
                f"in step '{step_name}'. Verify values were not corrupted."
            ),
            "value": f"{tc['before']} → {tc['after']}",
            "threshold": None,
        })

    # ── Duplicate spike ───────────────────────────────────────────────────────
    dup_before = diff.get("duplicate_before", 0) or 0
    dup_after = diff.get("duplicate_after", 0) or 0
    if dup_before > 0:
        dup_change_pct = (dup_after - dup_before) / dup_before * 100
        if dup_change_pct >= t["duplicate_spike_pct"]:
            anomalies.append({
                "type": "duplicate_spike",
                "severity": "warning",
                "column": None,
                "message": (
                    f"Duplicate rows increased by {dup_change_pct:.1f}% in step '{step_name}'. "
                    "This often happens after a join with non-unique keys."
                ),
                "value": dup_change_pct,
                "threshold": t["duplicate_spike_pct"],
            })
    elif dup_after > 10:
        anomalies.append({
            "type": "duplicates_appeared",
            "severity": "info",
            "column": None,
            "message": (
                f"{dup_after} duplicate rows appeared after step '{step_name}'."
            ),
            "value": dup_after,
            "threshold": None,
        })

    # ── Stat drift ────────────────────────────────────────────────────────────
    for col, sd in diff.get("stat_drift", {}).items():
        drift = sd.get("drift_pct")
        if drift is not None and abs(drift) >= t["stat_drift_pct"]:
            direction = "increased" if drift > 0 else "decreased"
            anomalies.append({
                "type": "stat_drift",
                "severity": "warning",
                "column": col,
                "message": (
                    f"Mean of '{col}' {direction} by {abs(drift):.1f}% in step '{step_name}'. "
                    f"Before: {sd.get('mean_before'):.3g}, After: {sd.get('mean_after'):.3g}."
                ),
                "value": drift,
                "threshold": t["stat_drift_pct"],
            })

    # ── Distribution shift (KS test) ──────────────────────────────────────────
    for col, ds in diff.get("distribution_shift", {}).items():
        ks = ds.get("ks_stat", 0)
        pv = ds.get("ks_pvalue", 1)
        if ks >= t["ks_stat_threshold"] and pv <= t["ks_pvalue_threshold"]:
            anomalies.append({
                "type": "distribution_shift",
                "severity": "warning",
                "column": col,
                "message": (
                    f"Column '{col}' shows a significant distribution shift in step '{step_name}' "
                    f"(KS={ks:.3f}, p={pv:.4f}). The value range or spread has changed substantially."
                ),
                "value": ks,
                "threshold": t["ks_stat_threshold"],
            })

    # ── Category disappearance ────────────────────────────────────────────────
    for col, cs in diff.get("category_shifts", {}).items():
        removed = cs.get("removed_values", [])
        if len(removed) >= 3:
            anomalies.append({
                "type": "category_disappear",
                "severity": "warning",
                "column": col,
                "message": (
                    f"Column '{col}' lost {len(removed)} distinct category value(s) "
                    f"in step '{step_name}': {', '.join(str(v) for v in removed[:5])}{'...' if len(removed) > 5 else ''}."
                ),
                "value": len(removed),
                "threshold": 3,
            })

    return anomalies
