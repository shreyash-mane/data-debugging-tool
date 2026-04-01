"""
Layer 8: Audit Logging
=======================
The audit log is a list of dicts accumulated throughout the pipeline.
This module provides helpers to summarise and serialise it for the API response.

The log is passed by reference through all layers, so no special collection
mechanism is needed — each layer appends directly.
"""

from __future__ import annotations

from typing import Any
from collections import Counter


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def summarise_audit(audit_log: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Return a structured summary of the full audit log.

    Shape
    -----
    {
        "total_changes": int,
        "by_layer": {"normalization": int, ...},
        "by_action": {"strip_whitespace": int, ...},
        "by_column": {"age": int, ...},
        "entries": [<audit entry>, ...]
    }
    """
    by_layer: Counter = Counter()
    by_action: Counter = Counter()
    by_column: Counter = Counter()

    for entry in audit_log:
        by_layer[entry.get("layer", "unknown")] += 1
        by_action[entry.get("action", "unknown")] += 1
        by_column[entry.get("column", "unknown")] += 1

    return {
        "total_changes": len(audit_log),
        "by_layer": dict(by_layer),
        "by_action": dict(by_action),
        "by_column": dict(by_column),
        "entries": audit_log,
    }


def filter_audit(
    audit_log: list[dict[str, Any]],
    layer: str | None = None,
    column: str | None = None,
    action: str | None = None,
) -> list[dict[str, Any]]:
    """Return a filtered view of the audit log."""
    result = audit_log
    if layer:
        result = [e for e in result if e.get("layer") == layer]
    if column:
        result = [e for e in result if e.get("column") == column]
    if action:
        result = [e for e in result if e.get("action") == action]
    return result
