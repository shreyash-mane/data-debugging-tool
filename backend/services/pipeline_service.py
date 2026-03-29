"""
pipeline_service.py — Business logic for pipeline and step CRUD.
"""

from __future__ import annotations
import json
from sqlalchemy.orm import Session
from database import Pipeline, PipelineStep, Dataset


def get_pipeline(db: Session, pipeline_id: int) -> Pipeline | None:
    return db.get(Pipeline, pipeline_id)


def list_pipelines(db: Session, dataset_id: int | None = None) -> list[Pipeline]:
    q = db.query(Pipeline)
    if dataset_id:
        q = q.filter(Pipeline.dataset_id == dataset_id)
    return q.order_by(Pipeline.created_at.desc()).all()


def create_pipeline(db: Session, name: str, dataset_id: int) -> Pipeline:
    dataset = db.get(Dataset, dataset_id)
    if not dataset:
        raise ValueError(f"Dataset {dataset_id} not found.")
    pipeline = Pipeline(name=name, dataset_id=dataset_id)
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline


def delete_pipeline(db: Session, pipeline_id: int) -> bool:
    p = db.get(Pipeline, pipeline_id)
    if not p:
        return False
    db.delete(p)
    db.commit()
    return True


def add_step(db: Session, pipeline_id: int, name: str, step_type: str,
             config_json: str, order: int, enabled: bool) -> PipelineStep:
    pipeline = db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise ValueError(f"Pipeline {pipeline_id} not found.")

    step = PipelineStep(
        pipeline_id=pipeline_id,
        name=name,
        step_type=step_type,
        config_json=config_json,
        order=order,
        enabled=enabled,
    )
    db.add(step)
    db.commit()
    db.refresh(step)
    return step


def update_step(db: Session, step_id: int, **kwargs) -> PipelineStep | None:
    step = db.get(PipelineStep, step_id)
    if not step:
        return None
    for k, v in kwargs.items():
        if v is not None:
            setattr(step, k, v)
    db.commit()
    db.refresh(step)
    return step


def delete_step(db: Session, step_id: int) -> bool:
    step = db.get(PipelineStep, step_id)
    if not step:
        return False
    db.delete(step)
    db.commit()
    return True


def reorder_steps(db: Session, pipeline_id: int, order_map: dict[int, int]):
    """Update the `order` field of each step by step_id → new_order mapping."""
    steps = db.query(PipelineStep).filter(PipelineStep.pipeline_id == pipeline_id).all()
    for step in steps:
        if step.id in order_map:
            step.order = order_map[step.id]
    db.commit()
