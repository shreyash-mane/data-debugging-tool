"""
Layer 9: Quality Scoring
=========================
Computes a 0–100 quality score for the dataframe before and after cleaning.

Scoring model (simple weighted penalties):
  - Missing values:       up to -30 pts  (scaled by pct missing)
  - Type issues:          -5 per column
  - Invalid values:       -5 per issue
  - Duplicate rows:       -10 flat
  - Duplicate IDs:        -10 flat

Confidence in the score itself:
  - high   → dataset large enough for reliable stats (≥ 50 rows)
  - medium → 10–49 rows
  - low    → < 10 rows
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .issue_detector import Issue


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_quality_score(
    df: pd.DataFrame,
    issues: list[Issue],
    label: str = "before",
) -> dict[str, Any]:
    """
    Returns:
    {
        "score": float (0-100),
        "label": str,
        "confidence": str,
        "penalties": [{"reason": str, "penalty": float}, ...]
    }
    """
    penalties: list[dict[str, Any]] = []
    total_rows = len(df)
    total_cells = total_rows * len(df.columns) if len(df.columns) else 1

    # --- Missing value penalty ---
    total_nulls = int(df.isna().sum().sum())
    null_pct = total_nulls / total_cells * 100 if total_cells else 0
    null_penalty = min(30, null_pct * 0.6)  # max -30
    if null_penalty > 0:
        penalties.append({"reason": "missing_values", "penalty": round(null_penalty, 2)})

    # --- Per-issue penalties ---
    issue_type_penalties = {
        "critical": 5.0,
        "warning": 2.0,
        "info": 0.5,
    }
    for issue in issues:
        p = issue_type_penalties.get(issue.severity, 1.0)
        penalties.append({
            "reason": f"{issue.issue_type} [{issue.column}]",
            "penalty": p,
        })

    total_penalty = sum(p["penalty"] for p in penalties)
    score = max(0.0, min(100.0, 100.0 - total_penalty))

    confidence = _score_confidence(total_rows)

    return {
        "score": round(score, 1),
        "label": label,
        "confidence": confidence,
        "penalties": penalties,
    }


def quality_score_pair(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    issues_before: list[Issue],
    issues_after: list[Issue],
) -> dict[str, Any]:
    """Compute before/after quality scores in one call."""
    before = compute_quality_score(df_before, issues_before, label="before_cleaning")
    after = compute_quality_score(df_after, issues_after, label="after_cleaning")

    return {
        "before_cleaning": before["score"],
        "after_cleaning": after["score"],
        "improvement": round(after["score"] - before["score"], 1),
        "confidence": before["confidence"],
        "detail": {"before": before, "after": after},
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _score_confidence(row_count: int) -> str:
    if row_count >= 50:
        return "high"
    if row_count >= 10:
        return "medium"
    return "low"
