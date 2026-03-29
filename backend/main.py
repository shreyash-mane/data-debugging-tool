"""
main.py — FastAPI entrypoint for the Data Debugging Tool backend.

Routes:
  POST   /api/datasets/upload
  GET    /api/datasets
  GET    /api/datasets/{id}
  DELETE /api/datasets/{id}

  POST   /api/pipelines
  GET    /api/pipelines
  GET    /api/pipelines/{id}
  DELETE /api/pipelines/{id}

  GET    /api/pipelines/{id}/steps
  POST   /api/pipelines/{id}/steps
  PUT    /api/steps/{step_id}
  DELETE /api/steps/{step_id}
  POST   /api/pipelines/{id}/reorder

  POST   /api/pipelines/{id}/run
  GET    /api/runs/{run_id}
  GET    /api/runs/{run_id}/snapshots
  GET    /api/snapshots/{snapshot_id}

  GET    /api/uploads          (list uploaded files — for join step UI)
"""

import json
import os
import shutil
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

import database as db_module
from database import (
    Dataset, Pipeline, PipelineStep, PipelineRun, StepSnapshot, get_db
)
from models import (
    DatasetOut, PipelineCreate, PipelineOut,
    StepCreate, StepUpdate, StepOut,
    PipelineRunOut, SnapshotOut, ReorderRequest,
)
from services import csv_service, pipeline_service
from services.execution_engine import execute_step
from services.diff_engine import compute_diff
from services.anomaly_detector import detect_anomalies
from services.explanation_engine import generate_explanations

# ── App setup ─────────────────────────────────────────────────────────────────

UPLOADS_DIR = Path("/tmp/uploads")
UPLOADS_DIR.mkdir(exist_ok=True, parents=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create DB tables and uploads directory
    db_module.create_tables()
    UPLOADS_DIR.mkdir(exist_ok=True)
    yield
    # Shutdown: nothing to clean up for SQLite MVP


app = FastAPI(title="Data Debugging Tool API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Dataset routes ─────────────────────────────────────────────────────────────

@app.post("/api/datasets/upload", response_model=DatasetOut)
async def upload_dataset(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(400, "Only .csv files are accepted.")

    safe_name = f"{uuid.uuid4().hex}_{file.filename}"
    file_path = UPLOADS_DIR / safe_name

    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        df = csv_service.load_csv(str(file_path))
    except Exception as e:
        file_path.unlink(missing_ok=True)
        raise HTTPException(400, f"Failed to parse CSV: {e}")

    snap = csv_service.build_snapshot_data(df)

    dataset = Dataset(
        name=file.filename,
        filename=file.filename,
        file_path=str(file_path),
        **{k: v for k, v in snap.items()},
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset


@app.get("/api/datasets", response_model=List[DatasetOut])
def list_datasets(db: Session = Depends(get_db)):
    return db.query(Dataset).order_by(Dataset.created_at.desc()).all()


@app.get("/api/datasets/{dataset_id}", response_model=DatasetOut)
def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    d = db.get(Dataset, dataset_id)
    if not d:
        raise HTTPException(404, "Dataset not found.")
    return d


@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    d = db.get(Dataset, dataset_id)
    if not d:
        raise HTTPException(404, "Dataset not found.")
    Path(d.file_path).unlink(missing_ok=True)
    db.delete(d)
    db.commit()
    return {"ok": True}


@app.get("/api/uploads")
def list_uploads():
    """Return filenames in the uploads directory — used by join step UI."""
    files = [f.name for f in UPLOADS_DIR.glob("*.csv")]
    return {"files": files}


# ── Pipeline routes ────────────────────────────────────────────────────────────

@app.post("/api/pipelines", response_model=PipelineOut)
def create_pipeline(body: PipelineCreate, db: Session = Depends(get_db)):
    try:
        p = pipeline_service.create_pipeline(db, body.name, body.dataset_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return p


@app.get("/api/pipelines", response_model=List[PipelineOut])
def list_pipelines(dataset_id: Optional[int] = None, db: Session = Depends(get_db)):
    return pipeline_service.list_pipelines(db, dataset_id)


@app.get("/api/pipelines/{pipeline_id}", response_model=PipelineOut)
def get_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    p = pipeline_service.get_pipeline(db, pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found.")
    return p


@app.delete("/api/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    if not pipeline_service.delete_pipeline(db, pipeline_id):
        raise HTTPException(404, "Pipeline not found.")
    return {"ok": True}


# ── Step routes ────────────────────────────────────────────────────────────────

@app.get("/api/pipelines/{pipeline_id}/steps", response_model=List[StepOut])
def list_steps(pipeline_id: int, db: Session = Depends(get_db)):
    p = db.get(Pipeline, pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found.")
    return sorted(p.steps, key=lambda s: s.order)


@app.post("/api/pipelines/{pipeline_id}/steps", response_model=StepOut)
def create_step(pipeline_id: int, body: StepCreate, db: Session = Depends(get_db)):
    try:
        step = pipeline_service.add_step(
            db, pipeline_id, body.name, body.step_type,
            body.config_json, body.order, body.enabled,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return step


@app.put("/api/steps/{step_id}", response_model=StepOut)
def update_step(step_id: int, body: StepUpdate, db: Session = Depends(get_db)):
    kwargs = body.model_dump(exclude_none=True)
    step = pipeline_service.update_step(db, step_id, **kwargs)
    if not step:
        raise HTTPException(404, "Step not found.")
    return step


@app.delete("/api/steps/{step_id}")
def delete_step(step_id: int, db: Session = Depends(get_db)):
    if not pipeline_service.delete_step(db, step_id):
        raise HTTPException(404, "Step not found.")
    return {"ok": True}


@app.post("/api/pipelines/{pipeline_id}/reorder")
def reorder_steps(pipeline_id: int, body: ReorderRequest, db: Session = Depends(get_db)):
    order_map = {s.step_id: s.order for s in body.steps}
    pipeline_service.reorder_steps(db, pipeline_id, order_map)
    return {"ok": True}


# ── Run routes ─────────────────────────────────────────────────────────────────

@app.post("/api/pipelines/{pipeline_id}/run", response_model=PipelineRunOut)
def run_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    """
    Execute all enabled steps sequentially.
    After each step:
      1. Save a StepSnapshot with statistics.
      2. Compute diff against the previous snapshot.
      3. Detect anomalies.
      4. Generate explanations.
    """
    pipeline = db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise HTTPException(404, "Pipeline not found.")

    dataset = db.get(Dataset, pipeline.dataset_id)
    if not dataset:
        raise HTTPException(400, "Dataset not found for pipeline.")

    # Create run record
    run = PipelineRun(pipeline_id=pipeline_id, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        df = csv_service.load_csv(dataset.file_path)
        steps = sorted([s for s in pipeline.steps if s.enabled], key=lambda s: s.order)

        # ── Snapshot 0: source dataset ────────────────────────────────────────
        snap_data = csv_service.build_snapshot_data(df)
        source_snap = StepSnapshot(
            run_id=run.id,
            step_id=None,
            step_index=0,
            step_name="Source Dataset",
            diff_json="{}",
            anomalies_json="[]",
            explanation_json="[]",
            **snap_data,
        )
        db.add(source_snap)
        db.flush()

        prev_df = df.copy()

        for idx, step in enumerate(steps, start=1):
            config = json.loads(step.config_json or "{}")

            # Execute the step
            result_df = execute_step(df, step.step_type, config, str(UPLOADS_DIR))

            # Build snapshot data
            snap_data = csv_service.build_snapshot_data(result_df)

            # Compute diff against previous step
            diff = compute_diff(prev_df, result_df)

            # Detect anomalies
            anomalies = detect_anomalies(diff, step.name, len(prev_df))

            # Generate explanations
            explanations = generate_explanations(anomalies, diff, step.step_type, step.name)

            snap = StepSnapshot(
                run_id=run.id,
                step_id=step.id,
                step_index=idx,
                step_name=step.name,
                diff_json=json.dumps(diff),
                anomalies_json=json.dumps(anomalies),
                explanation_json=json.dumps(explanations),
                **snap_data,
            )
            db.add(snap)

            prev_df = result_df
            df = result_df

        run.status = "success"
        run.finished_at = datetime.utcnow()

    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.finished_at = datetime.utcnow()
        db.commit()
        raise HTTPException(500, f"Pipeline execution failed: {e}")

    db.commit()
    db.refresh(run)
    return run


@app.get("/api/runs/{run_id}", response_model=PipelineRunOut)
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found.")
    return run


@app.get("/api/pipelines/{pipeline_id}/runs", response_model=List[PipelineRunOut])
def list_runs(pipeline_id: int, db: Session = Depends(get_db)):
    return (
        db.query(PipelineRun)
        .filter(PipelineRun.pipeline_id == pipeline_id)
        .order_by(PipelineRun.started_at.desc())
        .all()
    )


@app.get("/api/runs/{run_id}/snapshots", response_model=List[SnapshotOut])
def list_snapshots(run_id: int, db: Session = Depends(get_db)):
    snaps = (
        db.query(StepSnapshot)
        .filter(StepSnapshot.run_id == run_id)
        .order_by(StepSnapshot.step_index)
        .all()
    )
    return snaps


@app.get("/api/snapshots/{snapshot_id}", response_model=SnapshotOut)
def get_snapshot(snapshot_id: int, db: Session = Depends(get_db)):
    snap = db.get(StepSnapshot, snapshot_id)
    if not snap:
        raise HTTPException(404, "Snapshot not found.")
    return snap
