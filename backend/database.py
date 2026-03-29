"""
database.py — SQLite setup and ORM table definitions.
Uses SQLAlchemy 2.0 mapped_column style for clarity.
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, String, Integer, Float, Text, DateTime,
    ForeignKey, Boolean
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship, Session
)

DATABASE_URL = "sqlite:///./data_debugger.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False,
)


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "datasets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    filename: Mapped[str] = mapped_column(String(512))
    file_path: Mapped[str] = mapped_column(String(512))
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    col_count: Mapped[int] = mapped_column(Integer, default=0)
    schema_json: Mapped[str] = mapped_column(Text, default="{}")   # JSON: {col: dtype}
    stats_json: Mapped[str] = mapped_column(Text, default="{}")    # JSON: summary stats
    sample_json: Mapped[str] = mapped_column(Text, default="[]")   # JSON: first 10 rows
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    pipelines: Mapped[list["Pipeline"]] = relationship("Pipeline", back_populates="dataset")


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    dataset_id: Mapped[int] = mapped_column(Integer, ForeignKey("datasets.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="pipelines")
    steps: Mapped[list["PipelineStep"]] = relationship(
        "PipelineStep", back_populates="pipeline",
        order_by="PipelineStep.order", cascade="all, delete-orphan"
    )
    runs: Mapped[list["PipelineRun"]] = relationship("PipelineRun", back_populates="pipeline")


class PipelineStep(Base):
    __tablename__ = "pipeline_steps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    pipeline_id: Mapped[int] = mapped_column(Integer, ForeignKey("pipelines.id"))
    name: Mapped[str] = mapped_column(String(255))
    step_type: Mapped[str] = mapped_column(String(100))   # e.g. "drop_missing", "filter_rows"
    config_json: Mapped[str] = mapped_column(Text, default="{}")  # step-specific params
    order: Mapped[int] = mapped_column(Integer, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="steps")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    pipeline_id: Mapped[int] = mapped_column(Integer, ForeignKey("pipelines.id"))
    status: Mapped[str] = mapped_column(String(50), default="pending")  # pending/running/success/failed
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="runs")
    snapshots: Mapped[list["StepSnapshot"]] = relationship("StepSnapshot", back_populates="run")


class StepSnapshot(Base):
    __tablename__ = "step_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("pipeline_runs.id"))
    step_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("pipeline_steps.id"), nullable=True)
    step_index: Mapped[int] = mapped_column(Integer, default=0)   # 0 = original dataset
    step_name: Mapped[str] = mapped_column(String(255), default="source")
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    col_count: Mapped[int] = mapped_column(Integer, default=0)
    schema_json: Mapped[str] = mapped_column(Text, default="{}")
    stats_json: Mapped[str] = mapped_column(Text, default="{}")
    null_counts_json: Mapped[str] = mapped_column(Text, default="{}")
    sample_json: Mapped[str] = mapped_column(Text, default="[]")
    diff_json: Mapped[str] = mapped_column(Text, default="{}")        # diff from prev step
    anomalies_json: Mapped[str] = mapped_column(Text, default="[]")   # anomaly list
    explanation_json: Mapped[str] = mapped_column(Text, default="[]") # explanations
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    run: Mapped["PipelineRun"] = relationship("PipelineRun", back_populates="snapshots")


def get_db():
    """FastAPI dependency — yields a database session."""
    with Session(engine) as session:
        yield session


def create_tables():
    Base.metadata.create_all(bind=engine)
