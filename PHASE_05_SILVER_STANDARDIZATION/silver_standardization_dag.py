#!/usr/bin/env python3
"""Phase 05 Silver standardization DAG (pandas).

Responsibilities:
- Trigger Bronze ingestion tasks for students and courses
- Standardize Bronze rows into Silver clean datasets
- Write quarantine rows for hard quality failures (students path)
- Emit per-run Silver metrics
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys
import importlib.util
import logging

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure DAG-local utils package is importable in Airflow parser context.
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))


def _load_phase4_ingest_callable():
    candidates = [
        Path("/opt/airflow/dags/phase4/ingest_client_db_mongodb_phase4.py"),
        CURRENT_DIR.parent
        / "PHASE_04_BRONZE_STORAGE"
        / "ingest_client_db_mongodb_phase4.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            spec = importlib.util.spec_from_file_location(
                "phase4_ingest_module", str(candidate)
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module.ingest_collection
    raise ImportError("Could not load Phase 4 ingest callable from known paths")


ingest_collection_phase4 = _load_phase4_ingest_callable()

from utils.data_quality import standardize_columns, split_clean_quarantine


# ---------------------------------------------------------------------------
# Runtime paths and constants
# ---------------------------------------------------------------------------
BRONZE_ROOT = Path("/opt/airflow/data/bronze")
BRONZE_COURSES_ROOT = Path("/opt/airflow/data/bronze_stream/courses_events")
SILVER_ROOT = Path("/opt/airflow/data/silver")
COLLECTIONS = ["students"]
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------
def _bronze_courses_parquet_path(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (
        BRONZE_COURSES_ROOT
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / "events.jsonl"  ## jsonl?? It's json format to process data one record (line) at a time.
    )


def _bronze_courses_parquet_path_legacy(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (
        BRONZE_ROOT
        / "courses"
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / f"courses_{ds}_00.parquet"
    )


def _load_stream_courses_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return pd.DataFrame(rows)


def _silver_courses_path(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        SILVER_ROOT / "courses" / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path / "courses_clean_00.parquet"


###############################################
### fetches the location of silver parquet. ###
###############################################
def _silver_clean_path(collection: str, ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        SILVER_ROOT
        / collection
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{collection}_clean_00.parquet"


###############################################
### fetches the path of quarantine records ####
###############################################
def _silver_quarantine_path(collection: str, ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        SILVER_ROOT
        / "quarantine"
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path / f"quarantine_{collection}_00.parquet"


def _metrics_path(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        SILVER_ROOT / "metrics" / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path / "silver_metrics.json"


###############################################
#### get latest bronze parquet's file path ####
###############################################
def _bronze_parquet_path(collection: str, ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (
        BRONZE_ROOT
        / collection
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / f"{collection}_{ds}_00.parquet"
    )


# ---------------------------------------------------------------------------
# Standardization tasks
# ---------------------------------------------------------------------------
def standardize_collection(collection_name: str, ds: str, **context):
    """Standardize one collection from Bronze into Silver clean/quarantine outputs."""

    bronze_path = _bronze_parquet_path(
        collection_name, ds
    )  # fetches bronze parquet path
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    raw = pd.read_parquet(bronze_path)  # reads it raw!

    # This is where problems might arise!
    standardized = standardize_columns(raw, collection_name)

    clean, quarantine = split_clean_quarantine(standardized, str(bronze_path))

    clean_path = _silver_clean_path(collection_name, ds)

    clean.to_parquet(clean_path, index=False, compression="snappy")

    quarantine_count = 0
    quarantine_path = _silver_quarantine_path(collection_name, ds)
    if not quarantine.empty:
        quarantine.to_parquet(quarantine_path, index=False, compression="snappy")
        quarantine_count = len(quarantine)

    stats = {
        "collection": collection_name,
        "ds": ds,
        "bronze_input": str(bronze_path),
        "silver_output": str(clean_path),
        "quarantine_output": str(quarantine_path),
        "clean_count": int(len(clean)),
        "quarantine_count": int(quarantine_count),
    }

    logger.info("(Processed %s records)", int(len(clean)))

    # This pushes keyval to airflow's xcom (like a mini keypair db that tasks can read it)
    context["ti"].xcom_push(key=f"silver_metrics_{collection_name}", value=stats)
    return stats


def standardize_courses(ds: str, **context):
    """Build Silver course dimension-ready dataset from Bronze courses."""
    bronze_path = _bronze_courses_parquet_path(ds)
    logger.info(f"{bronze_path}. Does it exist? {bronze_path.exists()}")
    if bronze_path.exists():
        raw = _load_stream_courses_jsonl(bronze_path)
    else:
        # No courses to fetch
        # TODO: Why this loads????
        stats = {
            "collection": "courses",
            "ds": ds,
            "bronze_input": str(bronze_path),
            "silver_output": "N/A",
            "clean_count": 0,
            "quarantine_count": 0,
        }
        logger.info("(Processed 0 records)")
        context["ti"].xcom_push(key="silver_metrics_courses", value=stats)
        return stats
    out = pd.DataFrame()
    course_id = pd.Series([pd.NA] * len(raw), index=raw.index, dtype="string")
    if "course_id" in raw.columns:
        course_id = raw["course_id"].astype("string")
    if "_id" in raw.columns:
        course_id = course_id.fillna(raw["_id"].astype("string"))
    out["course_id"] = course_id.str.strip().replace("", pd.NA)
    if "course_name" in raw.columns:
        out["course_name"] = raw["course_name"].astype("string").str.strip()
    elif "name" in raw.columns:
        out["course_name"] = raw["name"].astype("string").str.strip()
    else:
        out["course_name"] = pd.Series([None] * len(raw), index=raw.index).astype(
            "string"
        )
    if "created_at" in raw.columns:
        out["created_at"] = pd.to_datetime(raw["created_at"], errors="coerce", utc=True)
    elif "createdAt" in raw.columns:
        out["created_at"] = pd.to_datetime(raw["createdAt"], errors="coerce", utc=True)
    else:
        out["created_at"] = pd.Series([pd.NaT] * len(raw), index=raw.index)

    if "updated_at" in raw.columns:
        out["updated_at"] = pd.to_datetime(raw["updated_at"], errors="coerce", utc=True)
    elif "updatedAt" in raw.columns:
        out["updated_at"] = pd.to_datetime(raw["updatedAt"], errors="coerce", utc=True)
    else:
        out["updated_at"] = pd.Series([pd.NaT] * len(raw), index=raw.index)
    # Keep one row per course_id for dimension loading.
    out = out.dropna(subset=["course_id"]).drop_duplicates(
        subset=["course_id"], keep="last"
    )

    out_path = _silver_courses_path(ds)
    logger.info("COURSES \n {}", out.head())
    out.to_parquet(out_path, index=False, compression="snappy")

    stats = {
        "collection": "courses",
        "ds": ds,
        "bronze_input": str(bronze_path),
        "silver_output": str(out_path),
        "clean_count": int(len(out)),
        "quarantine_count": 0,
    }
    logger.info("(Processed %s records)", int(len(out)))
    context["ti"].xcom_push(key="silver_metrics_courses", value=stats)
    return stats


def generate_silver_metrics(ds: str, **context):
    """Aggregate task-level metrics into one run artifact."""
    ti = context["ti"]
    students_stats = ti.xcom_pull(
        task_ids="standardize_students_collection", key="silver_metrics_students"
    )
    courses_stats = ti.xcom_pull(
        task_ids="standardize_courses_collection", key="silver_metrics_courses"
    )

    students_stats = students_stats or {"clean_count": 0, "quarantine_count": 0}
    courses_stats = courses_stats or {"clean_count": 0, "quarantine_count": 0}

    payload = {
        "ds": ds,
        "students": students_stats,
        "courses": courses_stats,
        "totals": {
            "clean_count": int(students_stats.get("clean_count", 0))
            + int(courses_stats.get("clean_count", 0)),
            "quarantine_count": int(students_stats.get("quarantine_count", 0)),
        },
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
    }

    metrics_path = _metrics_path(ds)
    metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="bronze_to_silver_standardization_daily",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",
    catchup=False,
    description="One DAG for Bronze ingestion and Silver standardization",
    tags=["phase-04", "phase-05", "bronze", "silver", "standardization"],
) as dag:
    bronze_students_ingest = PythonOperator(
        task_id="bronze_students_ingest",
        python_callable=ingest_collection_phase4,
        op_kwargs={"collection_name": "students", "ds": "{{ ds }}"},
    )

    standardize_students_collection = PythonOperator(
        task_id="standardize_students_collection",
        python_callable=standardize_collection,  # standardizes students collection to silver
        op_kwargs={"collection_name": "students", "ds": "{{ ds }}"},
    )

    standardize_courses_collection = PythonOperator(
        task_id="standardize_courses_collection",
        python_callable=standardize_courses,
        op_kwargs={"ds": "{{ ds }}"},
    )

    generate_silver_metrics_task = PythonOperator(
        task_id="generate_silver_metrics",
        python_callable=generate_silver_metrics,
        op_kwargs={"ds": "{{ ds }}"},
    )

    bronze_students_ingest >> standardize_students_collection
    [
        standardize_students_collection,
        standardize_courses_collection,
    ] >> generate_silver_metrics_task
