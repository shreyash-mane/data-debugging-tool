"""
explanation_engine.py — Generates human-readable root cause explanations
from anomalies and diff data.  No external API required — rule-based.

Also contains generate_ai_explanation() which calls Claude claude-sonnet-4-6 for
deeper, data-aware root cause analysis with actionable fix suggestions.
"""

from __future__ import annotations
import json
import os
import re
import textwrap


# Severity ranking for sorting
_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}


# ── AI Explanation ─────────────────────────────────────────────────────────────

def _build_prompt(
    step_name: str,
    sample_rows: list[dict],
    schema: dict,
    null_counts: dict,
    stats: dict,
    anomalies: list[dict],
    diff: dict,
) -> str:
    """Construct the analysis prompt sent to Claude."""

    # Schema summary
    schema_lines = "\n".join(
        f"  {col}: {dtype}" for col, dtype in schema.items()
    )

    # Null counts — only show columns with nulls
    null_lines = "\n".join(
        f"  {col}: {cnt} nulls" for col, cnt in null_counts.items() if cnt > 0
    ) or "  (none)"

    # Stats — condensed to key numerics
    stat_lines = []
    for col, s in stats.items():
        parts = [f"dtype={s.get('dtype','?')}", f"nulls={s.get('null_count',0)}"]
        if "min" in s:
            parts.append(f"min={s['min']}, max={s['max']}, mean={s.get('mean','?')}")
        if "top_values" in s:
            top = list(s["top_values"].items())[:3]
            parts.append("top=" + str({k: v for k, v in top}))
        stat_lines.append(f"  {col}: " + ", ".join(parts))
    stats_text = "\n".join(stat_lines) or "  (no stats)"

    # Sample rows as a compact table (first 20 rows, max 10 cols for brevity)
    visible_cols = list(sample_rows[0].keys())[:10] if sample_rows else []
    sample_lines = []
    if visible_cols:
        sample_lines.append(" | ".join(str(c)[:14] for c in visible_cols))
        sample_lines.append("-" * (17 * len(visible_cols)))
        for row in sample_rows[:20]:
            sample_lines.append(
                " | ".join(str(row.get(c, ""))[:14] for c in visible_cols)
            )
    sample_text = "\n".join(sample_lines) or "(no sample data)"

    # Anomalies
    anomaly_lines = "\n".join(
        f"  [{a.get('severity','?').upper()}] {a.get('type','?')}: {a.get('message','')}"
        for a in anomalies
    ) or "  (none detected by rule engine)"

    # Key diff metrics
    diff_lines = textwrap.dedent(f"""
      row_delta: {diff.get('row_delta', 0)} ({diff.get('row_delta_pct', 0)}%)
      duplicate_delta: {diff.get('duplicate_delta', 0)}
      type_changes: {list(diff.get('type_changes', {}).keys()) or 'none'}
      columns_added: {diff.get('columns_added', [])}
      columns_removed: {diff.get('columns_removed', [])}
    """).strip()

    return textwrap.dedent(f"""
        You are a senior data quality engineer reviewing a dataset after a pipeline transformation step called "{step_name}".

        ## Schema
        {schema_lines}

        ## Null counts (columns with missing values)
        {null_lines}

        ## Statistics per column
        {stats_text}

        ## Sample data (up to 20 rows)
        {sample_text}

        ## Anomalies flagged by the rule engine
        {anomaly_lines}

        ## Diff vs previous step
        {diff_lines}

        ---

        Your task: analyse the sample data and statistics above and identify ALL data quality issues.
        Pay special attention to:
        - Negative values in columns like age, salary, price, count, score (should be >= 0)
        - Text/string values in numeric columns (e.g. "thirty", "not_available", "N/A", "n/a", "null", "unknown")
        - Currency/formatting symbols in numeric columns (£, $, €, commas)
        - Invalid or impossible dates (month=13, future timestamps in historical data)
        - Inconsistent value formats within the same column
        - Duplicate rows
        - Columns that are mostly null (>40%)

        For each issue found, produce one entry in the output JSON array.
        Suggest the exact pipeline step to fix it using ONLY these step types and config shapes:

        filter_rows:     {{"column":"X","operator":">=","value":0}}   (operators: ==, !=, >, >=, <, <=, contains, notnull)
        fill_missing:    {{"column":"X","method":"auto"}}              (method: auto, mean, median, mode, value; omit column to fill all)
        change_dtype:    {{"column":"X","dtype":"float"}}              (dtype: float, int, str, datetime)
        remove_duplicates: {{"keep":"first"}}
        auto_clean:      {{}}                                          (strips symbols, fixes types, fills missing — zero config)
        rename_column:   {{"mappings":{{"old_name":"new_name"}}}}

        Return ONLY a valid JSON array — no markdown, no explanation outside the array.
        Each element must have exactly these fields:
        {{
          "issue": "One-line description",
          "root_cause": "Why this likely happened",
          "severity": "critical" | "warning" | "info",
          "example_values": ["val1", "val2"],
          "suggested_step_type": "filter_rows",
          "suggested_config": {{"column":"age","operator":">=","value":0}},
          "explanation": "What the step will do and why it fixes the issue"
        }}

        If there are no issues, return an empty array: []
    """).strip()


async def generate_ai_explanation(
    step_name: str,
    sample_rows: list[dict],
    schema: dict,
    null_counts: dict,
    stats: dict,
    anomalies: list[dict],
    diff: dict,
    api_key: str,
) -> list[dict]:
    """
    Call Claude claude-sonnet-4-6 to produce deep, data-aware root cause analysis
    with specific, actionable fix suggestions.

    Returns a list of dicts, each with:
      issue, root_cause, severity, example_values,
      suggested_step_type, suggested_config, explanation
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic package is not installed. Add it to requirements.txt."
        )

    prompt = _build_prompt(
        step_name, sample_rows, schema, null_counts, stats, anomalies, diff
    )

    client = anthropic.AsyncAnthropic(api_key=api_key)

    message = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=(
            "You are a senior data quality engineer. "
            "You respond only with valid JSON arrays — never with markdown or prose outside the array. "
            "Every response must start with [ and end with ]."
        ),
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip accidental markdown fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Claude returned non-JSON response: {raw[:300]}"
        ) from exc

    if not isinstance(result, list):
        raise ValueError(f"Expected a JSON array, got: {type(result).__name__}")

    # Validate and sanitise each item
    required_fields = {
        "issue", "root_cause", "severity",
        "example_values", "suggested_step_type", "suggested_config", "explanation",
    }
    cleaned: list[dict] = []
    for item in result:
        if not isinstance(item, dict):
            continue
        if not required_fields.issubset(item.keys()):
            # Backfill missing optional fields rather than dropping
            for f in required_fields:
                item.setdefault(f, "" if f not in ("example_values", "suggested_config") else ([] if f == "example_values" else {}))
        cleaned.append(item)

    return cleaned


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
