"""
Layer 7: Duplicate Handling
============================
Detects and removes duplicate rows / duplicate ID values.

Configurable via DuplicateConfig:
  - keep: "first" | "last" | "none"  (which duplicate to keep)
  - id_columns: list of columns to check for duplicate IDs separately
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class DuplicateConfig:
    keep: Literal["first", "last", "none"] = "first"
    id_columns: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def handle_duplicates(
    df: pd.DataFrame,
    schema: dict[str, Any],
    audit_log: list[dict],
    config: DuplicateConfig | None = None,
) -> pd.DataFrame:
    """
    Remove duplicate rows (and optionally deduplicate on ID columns).
    Returns the cleaned dataframe.
    """
    config = config or DuplicateConfig(
        id_columns=schema.get("id_columns", [])
    )
    df = df.copy()

    # Drop rows where ALL values are null/empty — these are structurally empty rows
    df = _drop_empty_rows(df, audit_log)
    df = _drop_duplicate_rows(df, config, audit_log)
    df = _handle_duplicate_ids(df, config, audit_log)

    return df


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------

def _drop_empty_rows(
    df: pd.DataFrame,
    audit_log: list[dict],
) -> pd.DataFrame:
    """
    Drop rows where all meaningful data columns are null/empty.

    A row is considered empty if every column EXCEPT id/index-like columns
    is null or whitespace-only. An id-only row with no other data is not a
    real record and should be removed.
    """
    check = df.replace(r"^\s*$", pd.NA, regex=True)

    # Identify columns that look like IDs
    id_like = [c for c in df.columns if re.search(r"\bid\b|_id$|^id_", c.lower())]
    data_cols = [c for c in df.columns if c not in id_like]

    if not data_cols:
        all_null_mask = check.isnull().all(axis=1)
    else:
        all_null_mask = check[data_cols].isnull().all(axis=1)

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
            f"(row indices: {empty_indices}). ID-only rows carry no information."
        ),
        "confidence": "high",
        "layer": "duplicate_handling",
    })

    return df

def _drop_duplicate_rows(
    df: pd.DataFrame,
    config: DuplicateConfig,
    audit_log: list[dict],
) -> pd.DataFrame:
    original_len = len(df)

    if config.keep == "none":
        # Drop ALL copies of duplicated rows
        df = df[~df.duplicated(keep=False)]
    else:
        df = df.drop_duplicates(keep=config.keep)

    dropped = original_len - len(df)
    if dropped > 0:
        audit_log.append({
            "column": "__all__",
            "row_index": None,
            "action": "drop_duplicate_rows",
            "from": f"{original_len} rows",
            "to": f"{len(df)} rows",
            "detail": f"Dropped {dropped} fully-duplicate row(s) (keep='{config.keep}')",
            "confidence": "high",
            "layer": "duplicate_handling",
        })

    return df


def _handle_duplicate_ids(
    df: pd.DataFrame,
    config: DuplicateConfig,
    audit_log: list[dict],
) -> pd.DataFrame:
    """
    For each ID column: log duplicate IDs but do NOT drop rows automatically
    (duplicate IDs require human review in most systems).
    We flag them instead.
    """
    for col in config.id_columns:
        if col not in df.columns:
            continue

        dup_mask = df[col].duplicated(keep=False) & df[col].notna()
        n_dup = int(dup_mask.sum())
        if n_dup == 0:
            continue

        examples = df.loc[dup_mask, col].unique()[:3].tolist()
        audit_log.append({
            "column": col,
            "row_index": None,
            "action": "flag_duplicate_ids",
            "from": None,
            "to": None,
            "detail": (
                f"{n_dup} rows share duplicate values in ID column '{col}'. "
                f"Examples: {examples}. Manual review recommended."
            ),
            "confidence": "high",
            "layer": "duplicate_handling",
        })

    return df
