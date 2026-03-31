"""
ai_analyzer.py — Claude-powered data quality analysis.

Analyses a pipeline run's source snapshot and produces per-column AI suggestions
with specific, actionable pipeline step configurations.

Main API:
  analyze_data_quality(snapshot_data, diff_data, anomalies) -> list[dict]
"""

from __future__ import annotations

import json
import re
import textwrap
from typing import Any


# ── Prompt construction ────────────────────────────────────────────────────────

def _format_sample_table(sample_rows: list[dict]) -> str:
    if not sample_rows:
        return "(no sample data)"
    cols = list(sample_rows[0].keys())[:10]
    lines = [" | ".join(str(c)[:12] for c in cols)]
    lines.append("-" * (15 * len(cols)))
    for row in sample_rows[:25]:
        lines.append(" | ".join(str(row.get(c, ""))[:12] for c in cols))
    return "\n".join(lines)


def _build_prompt(
    sample_rows: list[dict],
    schema: dict,
    null_counts: dict,
    stats: dict,
    anomalies: list[dict],
) -> str:
    schema_text = "\n".join(f"  {c}: {t}" for c, t in schema.items())

    null_text = "\n".join(
        f"  {c}: {n} nulls ({n / max(len(sample_rows), 1) * 100:.0f}% of sample)"
        for c, n in null_counts.items() if n > 0
    ) or "  none"

    stats_text = []
    for col, s in stats.items():
        parts = [f"dtype={s.get('dtype', '?')}", f"nulls={s.get('null_count', 0)}",
                 f"unique={s.get('unique_count', 0)}"]
        if "min" in s:
            parts += [f"min={s['min']}", f"max={s['max']}", f"mean={round(s.get('mean') or 0, 2)}"]
        if "top_values" in s:
            top = list(s["top_values"].items())[:4]
            parts.append("top_values=" + str({k: v for k, v in top}))
        stats_text.append(f"  {col}: " + ", ".join(str(p) for p in parts))
    stats_text = "\n".join(stats_text) or "  (none)"

    anomaly_text = "\n".join(
        f"  [{a.get('severity','?').upper()}] {a.get('type','?')} — {a.get('message','')}"
        for a in anomalies
    ) or "  none"

    sample_table = _format_sample_table(sample_rows)

    return textwrap.dedent(f"""
        You are a senior data quality engineer. Analyse this dataset and find ALL data quality issues.

        SCHEMA:
        {schema_text}

        NULL COUNTS:
        {null_text}

        COLUMN STATISTICS:
        {stats_text}

        SAMPLE DATA (up to 25 rows):
        {sample_table}

        ANOMALIES FROM RULE ENGINE:
        {anomaly_text}

        ---
        For EACH data quality issue found, produce one JSON entry.

        Rules for choosing the fix:
        • Numeric column has text like "thirty","N/A","not_available","null","n/a","unknown" →
          suggest change_dtype (dtype=float) — pandas will coerce invalid strings to NaN
        • Numeric column has impossible values (age=-5, age=150, salary=-1000) →
          suggest filter_rows with operator>= or <= to remove them; explain valid range
        • Numeric column has currency symbols (£45000, $1000) →
          suggest auto_clean (strips symbols automatically)
        • Mixed date formats →
          suggest change_dtype (dtype=datetime)
        • Duplicate rows exist →
          suggest remove_duplicates
        • Null rate > 30% in a column →
          suggest drop_missing for THAT column specifically (NOT fill)
        • Null rate <= 30% in numeric column →
          suggest fill_missing with method=median (robust to outliers)
        • Null rate <= 30% in categorical column →
          suggest fill_missing with method=mode

        IMPORTANT — suggested_config shapes (use EXACTLY these):
        filter_rows:      {{"column":"X","operator":">=","value":0}}
        fill_missing:     {{"column":"X","method":"median"}}   or   {{"column":"X","method":"mode"}}
        change_dtype:     {{"column":"X","dtype":"float"}}
        remove_duplicates:{{"keep":"first"}}
        drop_missing:     {{"columns":["X"],"how":"any"}}
        auto_clean:       {{}}
        rename_column:    {{"mappings":{{"old":"new"}}}}

        Return ONLY a valid JSON array — no markdown, no text outside the array.
        Each element must have EXACTLY these fields:
        {{
          "column": "column_name or null for dataset-wide issues",
          "issue_type": "text_in_numeric | impossible_value | currency_symbol | mixed_format | duplicate_rows | high_nulls | low_nulls | invalid_date",
          "issue_description": "Specific description with actual example values from the data",
          "confidence": "high | medium | low",
          "suggested_step_type": "filter_rows",
          "suggested_config": {{"column":"age","operator":">=","value":0}},
          "reasoning": "Why this fix is appropriate for this specific data"
        }}

        If no issues found return [].
    """).strip()


# ── Main function ──────────────────────────────────────────────────────────────

async def analyze_data_quality(
    snapshot_data: dict,
    diff_data: dict,
    anomalies: list[dict],
    api_key: str,
) -> list[dict]:
    """
    Call Claude claude-sonnet-4-6 to produce intelligent, data-aware quality analysis.

    snapshot_data keys: sample_rows, schema, null_counts, stats
    Returns list of dicts with:
      column, issue_type, issue_description, confidence,
      suggested_step_type, suggested_config, reasoning
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed — add it to requirements.txt")

    sample_rows: list[dict] = snapshot_data.get("sample_rows", [])
    schema: dict = snapshot_data.get("schema", {})
    null_counts: dict = snapshot_data.get("null_counts", {})
    stats: dict = snapshot_data.get("stats", {})

    prompt = _build_prompt(sample_rows, schema, null_counts, stats, anomalies)

    client = anthropic.AsyncAnthropic(api_key=api_key)
    message = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=(
            "You are a senior data quality engineer. "
            "Respond only with a valid JSON array. "
            "Never include markdown fences, prose, or any text outside the JSON array. "
            "Every response must start with [ and end with ]."
        ),
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip accidental markdown fences if Claude added them
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Claude returned non-JSON: {raw[:400]}") from exc

    if not isinstance(result, list):
        raise ValueError(f"Expected JSON array, got {type(result).__name__}")

    required = {
        "column", "issue_type", "issue_description",
        "confidence", "suggested_step_type", "suggested_config", "reasoning",
    }
    cleaned: list[dict] = []
    for item in result:
        if not isinstance(item, dict):
            continue
        for field in required:
            item.setdefault(field, "" if field not in ("suggested_config",) else {})
        cleaned.append(item)

    return cleaned
