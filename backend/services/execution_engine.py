"""
execution_engine.py — Executes pipeline steps on a DataFrame.

Each step type is a pure function: (df, config) -> df.
Unsupported or unsafe step types raise ValueError with a clear message.

NOTE on "custom expression" step type:
  Custom pandas expressions are intentionally NOT supported in this MVP.
  Executing arbitrary user code is a significant security risk (code injection,
  server resource abuse). The proper implementation would require a sandboxed
  subprocess or WebAssembly runtime, which is beyond this MVP scope.
  Users can achieve custom logic by composing the supported step types.
"""

from __future__ import annotations
import json
import pandas as pd
import numpy as np


# ── Step dispatcher ────────────────────────────────────────────────────────────

SUPPORTED_STEPS = {
    "drop_missing",
    "fill_missing",
    "rename_column",
    "change_dtype",
    "filter_rows",
    "select_columns",
    "sort_values",
    "remove_duplicates",
    "add_computed_column",
    "join",
    "group_aggregate",
}


def execute_step(df: pd.DataFrame, step_type: str, config: dict, uploads_dir: str) -> pd.DataFrame:
    """
    Execute a single transformation step.
    Returns the transformed DataFrame.
    Raises ValueError for invalid config or unsupported step type.
    """
    handlers = {
        "drop_missing": _drop_missing,
        "fill_missing": _fill_missing,
        "rename_column": _rename_column,
        "change_dtype": _change_dtype,
        "filter_rows": _filter_rows,
        "select_columns": _select_columns,
        "sort_values": _sort_values,
        "remove_duplicates": _remove_duplicates,
        "add_computed_column": _add_computed_column,
        "join": lambda df, c: _join(df, c, uploads_dir),
        "group_aggregate": _group_aggregate,
    }

    if step_type not in handlers:
        raise ValueError(
            f"Unknown step type '{step_type}'. "
            f"Supported types: {sorted(SUPPORTED_STEPS)}"
        )

    return handlers[step_type](df, config)


# ── Step implementations ───────────────────────────────────────────────────────

def _drop_missing(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - columns: list[str] | None  → only consider these columns (None = all)
      - how: "any" | "all"         → default "any"
      - thresh: int | None         → minimum non-null values required per row
    """
    columns = config.get("columns") or None
    how = config.get("how", "any")
    thresh = config.get("thresh")

    kwargs: dict = {"how": how}
    if columns:
        kwargs["subset"] = columns
    if thresh is not None:
        kwargs.pop("how", None)
        kwargs["thresh"] = int(thresh)

    return df.dropna(**kwargs).reset_index(drop=True)


def _fill_missing(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - column: str           → column to fill
      - method: "value" | "mean" | "median" | "mode" | "ffill" | "bfill"
      - value: any            → used when method == "value"
    """
    col = config.get("column")
    method = config.get("method", "value")
    fill_value = config.get("value", "")

    if not col:
        raise ValueError("fill_missing requires 'column'.")
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found.")

    df = df.copy()
    if method == "mean":
        df[col] = df[col].fillna(df[col].mean())
    elif method == "median":
        df[col] = df[col].fillna(df[col].median())
    elif method == "mode":
        mode_val = df[col].mode()
        df[col] = df[col].fillna(mode_val[0] if len(mode_val) else None)
    elif method == "ffill":
        df[col] = df[col].ffill()
    elif method == "bfill":
        df[col] = df[col].bfill()
    else:
        df[col] = df[col].fillna(fill_value)

    return df


def _rename_column(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - mappings: dict[str, str]  → {old_name: new_name, ...}
    """
    mappings = config.get("mappings", {})
    if not mappings:
        raise ValueError("rename_column requires 'mappings' dict.")
    return df.rename(columns=mappings)


def _change_dtype(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - column: str
      - dtype: "int" | "float" | "str" | "bool" | "datetime"
    """
    col = config.get("column")
    dtype = config.get("dtype")
    if not col or not dtype:
        raise ValueError("change_dtype requires 'column' and 'dtype'.")
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found.")

    df = df.copy()
    try:
        if dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "str":
            df[col] = df[col].astype(str)
        elif dtype == "bool":
            df[col] = df[col].astype(bool)
        elif dtype == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            raise ValueError(f"Unsupported dtype '{dtype}'. Use: int, float, str, bool, datetime.")
    except Exception as e:
        raise ValueError(f"Type conversion failed for column '{col}': {e}")

    return df


def _filter_rows(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - column: str
      - operator: "==" | "!=" | ">" | ">=" | "<" | "<=" | "contains" | "startswith" | "isnull" | "notnull"
      - value: any
    """
    col = config.get("column")
    op = config.get("operator", "==")
    value = config.get("value")

    if not col:
        raise ValueError("filter_rows requires 'column'.")
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found.")

    series = df[col]

    try:
        if op == "==":
            mask = series == value
        elif op == "!=":
            mask = series != value
        elif op == ">":
            mask = series > float(value)
        elif op == ">=":
            mask = series >= float(value)
        elif op == "<":
            mask = series < float(value)
        elif op == "<=":
            mask = series <= float(value)
        elif op == "contains":
            mask = series.astype(str).str.contains(str(value), na=False)
        elif op == "startswith":
            mask = series.astype(str).str.startswith(str(value), na=False)
        elif op == "isnull":
            mask = series.isna()
        elif op == "notnull":
            mask = series.notna()
        else:
            raise ValueError(f"Unknown operator '{op}'.")
    except (TypeError, ValueError) as e:
        raise ValueError(f"Filter failed: {e}")

    return df[mask].reset_index(drop=True)


def _select_columns(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - columns: list[str]
    """
    columns = config.get("columns", [])
    if not columns:
        raise ValueError("select_columns requires a non-empty 'columns' list.")
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}")
    return df[columns]


def _sort_values(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - columns: list[str]
      - ascending: bool | list[bool]  (default True)
    """
    columns = config.get("columns", [])
    ascending = config.get("ascending", True)
    if not columns:
        raise ValueError("sort_values requires 'columns'.")
    return df.sort_values(by=columns, ascending=ascending).reset_index(drop=True)


def _remove_duplicates(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - columns: list[str] | None  → subset to consider (None = all)
      - keep: "first" | "last" | False  (default "first")
    """
    columns = config.get("columns") or None
    keep = config.get("keep", "first")
    if keep == "false" or keep is False:
        keep = False
    return df.drop_duplicates(subset=columns, keep=keep).reset_index(drop=True)


def _add_computed_column(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Adds a new column via a safe, limited set of operations.
    config keys:
      - new_column: str
      - operation: "add" | "subtract" | "multiply" | "divide" | "concat" | "constant"
      - col_a: str         → first operand column
      - col_b: str | None  → second operand column (or use 'constant_value')
      - constant_value: any
    """
    new_col = config.get("new_column")
    operation = config.get("operation", "add")
    col_a = config.get("col_a")
    col_b = config.get("col_b")
    const_val = config.get("constant_value")

    if not new_col or not col_a:
        raise ValueError("add_computed_column requires 'new_column' and 'col_a'.")
    if col_a not in df.columns:
        raise ValueError(f"Column '{col_a}' not found.")

    df = df.copy()
    a = df[col_a]

    if operation == "constant":
        df[new_col] = const_val
        return df

    if col_b:
        if col_b not in df.columns:
            raise ValueError(f"Column '{col_b}' not found.")
        b = df[col_b]
    else:
        b = const_val

    if operation == "add":
        df[new_col] = a + b
    elif operation == "subtract":
        df[new_col] = a - b
    elif operation == "multiply":
        df[new_col] = a * b
    elif operation == "divide":
        df[new_col] = a / b
    elif operation == "concat":
        df[new_col] = a.astype(str) + str(b if col_b is None else df[col_b])
    else:
        raise ValueError(f"Unknown operation '{operation}'.")

    return df


def _join(df: pd.DataFrame, config: dict, uploads_dir: str) -> pd.DataFrame:
    """
    config keys:
      - right_dataset_path: str   → filename of the second CSV in uploads dir
      - on: str | list[str]       → join key(s)
      - how: "inner" | "left" | "right" | "outer"  (default "inner")
    """
    import os
    right_path = config.get("right_dataset_path")
    on = config.get("on")
    how = config.get("how", "inner")

    if not right_path or not on:
        raise ValueError("join requires 'right_dataset_path' and 'on'.")

    full_path = os.path.join(uploads_dir, right_path)
    if not os.path.exists(full_path):
        raise ValueError(f"Right dataset not found: {right_path}")

    right_df = pd.read_csv(full_path, low_memory=False)

    keys = on if isinstance(on, list) else [on]
    missing_left = [k for k in keys if k not in df.columns]
    missing_right = [k for k in keys if k not in right_df.columns]
    if missing_left:
        raise ValueError(f"Join key(s) not in left dataset: {missing_left}")
    if missing_right:
        raise ValueError(f"Join key(s) not in right dataset: {missing_right}")

    return df.merge(right_df, on=on, how=how).reset_index(drop=True)


def _group_aggregate(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    config keys:
      - group_by: list[str]
      - aggregations: dict[str, str]   → {col: "sum"|"mean"|"count"|"min"|"max"|"first"|"last"}
    """
    group_by = config.get("group_by", [])
    aggregations = config.get("aggregations", {})

    if not group_by:
        raise ValueError("group_aggregate requires 'group_by'.")
    if not aggregations:
        raise ValueError("group_aggregate requires 'aggregations'.")

    missing = [c for c in group_by if c not in df.columns]
    if missing:
        raise ValueError(f"Group-by columns not found: {missing}")

    return df.groupby(group_by, as_index=False).agg(aggregations).reset_index(drop=True)
