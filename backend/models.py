"""
models.py — Pydantic v2 request/response schemas.
These are separate from the SQLAlchemy ORM models in database.py.
"""

from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict
from datetime import datetime


# ── Dataset ──────────────────────────────────────────────────────────────────

class DatasetOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    filename: str
    row_count: int
    col_count: int
    schema_json: str
    stats_json: str
    sample_json: str
    created_at: datetime


# ── Pipeline ──────────────────────────────────────────────────────────────────

class PipelineCreate(BaseModel):
    name: str
    dataset_id: int


class PipelineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    dataset_id: int
    created_at: datetime
    updated_at: datetime


# ── Steps ─────────────────────────────────────────────────────────────────────

class StepCreate(BaseModel):
    name: str
    step_type: str
    config_json: str = "{}"
    order: int = 0
    enabled: bool = True


class StepUpdate(BaseModel):
    name: Optional[str] = None
    step_type: Optional[str] = None
    config_json: Optional[str] = None
    order: Optional[int] = None
    enabled: Optional[bool] = None


class StepOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    pipeline_id: int
    name: str
    step_type: str
    config_json: str
    order: int
    enabled: bool


# ── Runs ──────────────────────────────────────────────────────────────────────

class PipelineRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    pipeline_id: int
    status: str
    error_message: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]


# ── Snapshots ─────────────────────────────────────────────────────────────────

class SnapshotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: int
    step_id: Optional[int]
    step_index: int
    step_name: str
    row_count: int
    col_count: int
    schema_json: str
    stats_json: str
    null_counts_json: str
    sample_json: str
    diff_json: str
    anomalies_json: str
    explanation_json: str
    created_at: datetime


# ── Reorder ───────────────────────────────────────────────────────────────────

class StepOrder(BaseModel):
    step_id: int
    order: int


class ReorderRequest(BaseModel):
    steps: list[StepOrder]
