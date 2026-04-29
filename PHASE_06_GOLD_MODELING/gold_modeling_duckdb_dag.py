#!/usr/bin/env python3
"""Phase 06 Gold modeling DAG (DuckDB).

Pipeline scope:
- Executes downstream of Bronze and Silver tasks
- Builds analytical dimensions and facts in DuckDB
- Produces paid-student and course-enrollment KPIs
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import importlib.util
import sys
import logging

import duckdb
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


SILVER_ROOT = Path("/opt/airflow/data/silver")
GOLD_ROOT = Path("/opt/airflow/data/gold")
DUCKDB_PATH = Path(
    Variable.get("duckdb_path", default_var="/opt/airflow/data/gold/gold.duckdb")
)
logger = logging.getLogger(__name__)


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))


# ---------------------------------------------------------------------------
# Callable loader utilities
# ---------------------------------------------------------------------------
def _load_callable(module_name: str, file_candidates: list[Path], callable_name: str):
    for candidate in file_candidates:
        if candidate.exists():
            spec = importlib.util.spec_from_file_location(module_name, str(candidate))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return getattr(module, callable_name)
    raise ImportError(
        f"Unable to load {callable_name} from candidates: {file_candidates}"
    )


ingest_collection_phase4 = _load_callable(
    "phase4_ingest_module",
    [
        Path("/opt/airflow/dags/phase4/ingest_client_db_mongodb_phase4.py"),
        CURRENT_DIR.parent
        / "PHASE_04_BRONZE_STORAGE"
        / "ingest_client_db_mongodb_phase4.py",
    ],
    "ingest_collection",
)

standardize_collection_phase5 = _load_callable(
    "phase5_silver_module",
    [
        Path("/opt/airflow/dags/phase5/silver_standardization_dag.py"),
        CURRENT_DIR.parent
        / "PHASE_05_SILVER_STANDARDIZATION"
        / "silver_standardization_dag.py",
    ],
    "standardize_collection",
)

standardize_courses_phase5 = _load_callable(
    "phase5_silver_module_courses",
    [
        Path("/opt/airflow/dags/phase5/silver_standardization_dag.py"),
        CURRENT_DIR.parent
        / "PHASE_05_SILVER_STANDARDIZATION"
        / "silver_standardization_dag.py",
    ],
    "standardize_courses",
)

silver_metrics = _load_callable(
    "phase5_silver_module_metrics",
    [
        Path("/opt/airflow/dags/phase5/silver_standardization_dag.py"),
        CURRENT_DIR.parent
        / "PHASE_05_SILVER_STANDARDIZATION"
        / "silver_standardization_dag.py",
    ],
    "generate_silver_metrics",)
# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------
def _silver_students_path(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (
        SILVER_ROOT
        / "students"
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / "students_clean_00.parquet"
    )


def _silver_courses_path(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (
        SILVER_ROOT
        / "courses"
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / "courses_clean_00.parquet"
    )


def _metrics_path(ds: str) -> Path:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    path = (
        GOLD_ROOT / "metrics" / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path / "gold_metrics.json"


# ---------------------------------------------------------------------------
# Gold schema bootstrap
# ---------------------------------------------------------------------------
def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    # Creates DIM_DATE
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            date_value DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            week_of_year INTEGER
        )
        """
    )

    # Creates DIM_CITY
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_city (
            city_key VARCHAR PRIMARY KEY,
            city_name VARCHAR,
            region_name VARCHAR,
            city_normalized VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Creates DIM_STUDENT.
    # TODO: Change student_key to int autoincrement?
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_student (
            student_key VARCHAR PRIMARY KEY,
            student_id VARCHAR,
            account_id VARCHAR,
            full_name VARCHAR,
            email VARCHAR,
            phone_number VARCHAR,
            city_key VARCHAR,
            city_name VARCHAR,
            region_name VARCHAR,
            education_level VARCHAR,
            account_status VARCHAR,
            is_contacted BOOLEAN,
            is_paid_student BOOLEAN,
            enrolled_courses_count INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            processed_at TIMESTAMP,
            source_batch_id VARCHAR,
            last_partition_key VARCHAR,
            is_active BOOLEAN,
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Creates DIM_COURSE
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_course (
            course_key VARCHAR PRIMARY KEY,
            course_id VARCHAR,
            course_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # FACT_STUDENT_DAILY
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_student_daily (
            date_key INTEGER,
            city_key VARCHAR,
            student_total_count BIGINT,
            student_paid_count BIGINT,
            paid_rate DOUBLE,
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date_key, city_key)
        )
        """
    )

    # mv????
    # TODO: DELETE!
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mv_paid_students_daily (
            date_key INTEGER,
            city_key VARCHAR,
            student_total_count BIGINT,
            student_paid_count BIGINT,
            paid_rate DOUBLE,
            refreshed_at TIMESTAMP,
            PRIMARY KEY (date_key, city_key)
        )
        """
    )

    # TODO: remove event_id and make PRIMARY KEY (date_key, city_key, student_key, course_key)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_course_enrollment_event (
            event_id VARCHAR PRIMARY KEY,
            date_key INTEGER,
            event_time TIMESTAMP,
            student_key VARCHAR,
            student_id VARCHAR,
            city_key VARCHAR,
            course_key VARCHAR,
            course_id VARCHAR,
            access_type VARCHAR,
            end_at TIMESTAMP,
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN,
        )
        """
    )

    # TODO: is this necessary?
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mv_course_enrollment_region_daily (
            date_key INTEGER,
            region_name VARCHAR,
            course_key VARCHAR,
            course_name VARCHAR,
            enrolled_students_count BIGINT,
            refreshed_at TIMESTAMP,
            PRIMARY KEY (date_key, region_name, course_key)
        )
        """
    )


def build_gold_for_ds(ds: str, **context):
    """Build Gold dimensions/facts and refresh materialized KPI tables for one date."""
    students_path = _silver_students_path(ds)
    courses_path = _silver_courses_path(ds)
    if not students_path.exists():
        raise FileNotFoundError(f"Missing Silver students file: {students_path}")
    if not courses_path.exists():
        logger.warning(f"Missing Silver courses file: {courses_path}")

    GOLD_ROOT.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_PATH))

    try:
        _ensure_schema(conn)

        date_key = int(ds.replace("-", ""))

        # Date dimension: one row per execution date. OK
        conn.execute(
            """
            INSERT OR REPLACE INTO dim_date
            SELECT
                ?,
                CAST(? AS DATE),
                EXTRACT(YEAR FROM CAST(? AS DATE))::INTEGER,
                EXTRACT(MONTH FROM CAST(? AS DATE))::INTEGER,
                EXTRACT(DAY FROM CAST(? AS DATE))::INTEGER,
                EXTRACT(DAYOFWEEK FROM CAST(? AS DATE))::INTEGER,
                EXTRACT(WEEK FROM CAST(? AS DATE))::INTEGER
            """,
            [date_key, ds, ds, ds, ds, ds, ds],
        )

        # City dimension from current student slice. OK
        conn.execute(
            """
            WITH normalized AS (
                SELECT DISTINCT UPPER(TRIM(COALESCE(city, 'UNKNOWN'))) AS city_normalized
                FROM read_parquet(?)
            )
            INSERT OR REPLACE INTO dim_city (city_key, city_name, region_name, city_normalized, created_at)
            SELECT
                md5(city_normalized) AS city_key,
                CASE
                    WHEN strpos(city_normalized, ',') > 0 THEN trim(split_part(city_normalized, ',', 2))
                    ELSE city_normalized
                END AS city_name,
                CASE
                    WHEN strpos(city_normalized, ',') > 0 THEN trim(split_part(city_normalized, ',', 1))
                    ELSE city_normalized
                END AS region_name,
                city_normalized,
                CURRENT_TIMESTAMP
            FROM normalized
            """,
            [str(students_path)],
        )

        # Course dimension from Silver courses.
        if courses_path.exists():
            conn.execute(
                """
                INSERT OR REPLACE INTO dim_course (course_key, course_id, course_name, created_at)
                SELECT
                    md5(COALESCE(course_id, 'UNKNOWN')) AS course_key,
                    course_id,
                    COALESCE(course_name, 'UNKNOWN') AS course_name,
                    CURRENT_TIMESTAMP
                FROM read_parquet(?)
                """,
                [str(courses_path)],
            )

        # Student dimension keeps latest profile-level attributes.
        conn.execute(
            """
            INSERT OR REPLACE INTO dim_student (
                student_key,
                student_id,
                account_id,
                full_name,
                email,
                phone_number,
                city_key,
                city_name,
                region_name,
                education_level,
                account_status,
                is_contacted,
                is_paid_student,
                enrolled_courses_count,
                created_at,
                updated_at,
                processed_at,
                source_batch_id,
                last_partition_key,
                is_active,
                load_ts
            )
            WITH latest_student AS (
                SELECT
                    CAST(entity_id AS VARCHAR) AS student_id,
                    CAST(account_id AS VARCHAR) AS account_id,
                    CAST(full_name AS VARCHAR) AS full_name,
                    CAST(NULL AS VARCHAR) AS email,
                    CAST(phone_number AS VARCHAR) AS phone_number,
                    UPPER(TRIM(COALESCE(city, 'UNKNOWN'))) AS city_normalized,
                    CASE
                        WHEN strpos(UPPER(TRIM(COALESCE(city, 'UNKNOWN'))), ',') > 0
                            THEN trim(split_part(UPPER(TRIM(COALESCE(city, 'UNKNOWN'))), ',', 2))
                        ELSE UPPER(TRIM(COALESCE(city, 'UNKNOWN')))
                    END AS city_name,
                    CASE
                        WHEN strpos(UPPER(TRIM(COALESCE(city, 'UNKNOWN'))), ',') > 0
                            THEN trim(split_part(UPPER(TRIM(COALESCE(city, 'UNKNOWN'))), ',', 1))
                        ELSE UPPER(TRIM(COALESCE(city, 'UNKNOWN')))
                    END AS region_name,
                    CAST(education_level AS VARCHAR) AS education_level,
                    CAST(account_status AS VARCHAR) AS account_status,
                    COALESCE(CAST(is_contacted AS BOOLEAN), FALSE) AS is_contacted,
                    COALESCE(CAST(is_paid_student AS BOOLEAN), FALSE) AS is_paid_student,
                    COALESCE(CAST(enrolled_courses_count AS INTEGER), 0) AS enrolled_courses_count,
                    CAST(created_at AS TIMESTAMP) AS created_at,
                    CAST(updated_at AS TIMESTAMP) AS updated_at,
                    CAST(processed_at AS TIMESTAMP) AS processed_at,
                    CAST(bronze_batch_id AS VARCHAR) AS source_batch_id,
                    CAST(_partition_key AS VARCHAR) AS last_partition_key,
                    ROW_NUMBER() OVER (
                        PARTITION BY entity_id
                        ORDER BY updated_at DESC NULLS LAST, processed_at DESC NULLS LAST
                    ) AS rn
                FROM read_parquet(?)
            )
            SELECT
                md5(COALESCE(student_id, 'UNKNOWN')) AS student_key,
                student_id,
                account_id,
                full_name,
                email,
                phone_number,
                md5(city_normalized) AS city_key,
                city_name,
                region_name,
                education_level,
                account_status,
                is_contacted,
                is_paid_student,
                enrolled_courses_count,
                created_at,
                updated_at,
                processed_at,
                source_batch_id,
                last_partition_key,
                CASE WHEN lower(COALESCE(account_status, '')) = 'active' THEN TRUE ELSE FALSE END AS is_active,
                CURRENT_TIMESTAMP
            FROM latest_student
            WHERE rn = 1
            """,
            [str(students_path)],
        )

        # Aggregate fact for paid/unpaid KPIs at city/day grain.
        conn.execute("DELETE FROM fact_student_daily WHERE date_key = ?", [date_key])
        conn.execute(
            """
            INSERT INTO fact_student_daily (date_key, city_key, student_total_count, student_paid_count, paid_rate, load_ts)
            SELECT
                ?,
                md5(UPPER(TRIM(COALESCE(city, 'UNKNOWN')))) AS city_key,
                COUNT(*) AS student_total_count,
                SUM(CASE WHEN COALESCE(is_paid_student, FALSE) THEN 1 ELSE 0 END) AS student_paid_count,
                CASE
                    WHEN COUNT(*) = 0 THEN 0.0
                    ELSE SUM(CASE WHEN COALESCE(is_paid_student, FALSE) THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)::DOUBLE
                END AS paid_rate,
                CURRENT_TIMESTAMP
            FROM read_parquet(?)
            GROUP BY 2
            """,
            [date_key, str(students_path)],
        )

        # Materialized paid KPI table for BI queries.
        conn.execute(
            "DELETE FROM mv_paid_students_daily WHERE date_key = ?", [date_key]
        )
        conn.execute(
            """
            INSERT INTO mv_paid_students_daily (date_key, city_key, student_total_count, student_paid_count, paid_rate, refreshed_at)
            SELECT
                date_key,
                city_key,
                student_total_count,
                student_paid_count,
                paid_rate,
                CURRENT_TIMESTAMP
            FROM fact_student_daily
            WHERE date_key = ?
            """,
            [date_key],
        )

        # Transactional-style course enrollment fact extracted from structured enrolled_courses.
        conn.execute(
            "DELETE FROM fact_course_enrollment_event WHERE date_key = ?", [date_key]
        )
        conn.execute(
            """
            INSERT INTO fact_course_enrollment_event (
                event_id,
                date_key,
                event_time,
                student_key,
                student_id,
                city_key,
                course_key,
                course_id,
                access_type,
                end_at,
                load_ts,
                is_active
            )
            WITH source AS (
                SELECT
                    md5(UPPER(TRIM(COALESCE(s.city, 'UNKNOWN')))) AS city_key,
                    CAST(s.entity_id AS VARCHAR) AS student_id,
                    COALESCE(
                        TRY_CAST(s.enrolled_courses AS JSON),
                        TRY_CAST(CAST(s.enrolled_courses AS VARCHAR) AS JSON),
                        CAST('[]' AS JSON)
                    ) AS enrolled_courses_json
                FROM read_parquet(?) s
            ), exploded AS (
                SELECT
                    city_key,
                    student_id,
                    unnest(COALESCE(json_extract(enrolled_courses_json, '$[*]'), [])) AS course_json
                FROM source
            ), normalized AS (
                SELECT
                    city_key,
                    student_id,
                    CAST(
                        NULLIF(
                            TRIM(
                                COALESCE(
                                    json_extract_string(course_json, '$.courseId'),
                                    json_extract_string(course_json, '$.course_id'),
                                    json_extract_string(course_json, '$.id'),
                                    json_extract_string(course_json, '$._id')
                                )
                            ),
                            ''
                        ) AS VARCHAR
                    ) AS course_id,
                    TRY_CAST(
                        COALESCE(
                            json_extract_string(course_json, '$.enrolledAt'),
                            json_extract_string(course_json, '$.enrolled_at'),
                            json_extract_string(course_json, '$.createdAt'),
                            json_extract_string(course_json, '$.created_at')
                        ) AS TIMESTAMP
                    ) AS event_time,
                    TRY_CAST(
                        COALESCE(
                            json_extract_string(course_json, '$.endAt'),
                            json_extract_string(course_json, '$.end_at'),
                            json_extract_string(course_json, '$.expiresAt'),
                            json_extract_string(course_json, '$.expires_at')
                        ) AS TIMESTAMP
                    ) AS end_at,
                    CAST(
                        NULLIF(
                            TRIM(
                                COALESCE(
                                    json_extract_string(course_json, '$.accessType'),
                                    json_extract_string(course_json, '$.access_type'),
                                    json_extract_string(course_json, '$.type')
                                )
                            ),
                            ''
                        ) AS VARCHAR
                    ) AS access_type
                FROM exploded
            )
            SELECT
                md5(COALESCE(student_id, 'UNKNOWN') || '|' || COALESCE(course_id, 'UNKNOWN') || '|' || COALESCE(CAST(event_time AS VARCHAR), CAST(? AS VARCHAR))) AS event_id,
                ?,
                event_time,
                md5(COALESCE(student_id, 'UNKNOWN')) AS student_key,
                student_id,
                city_key,
                md5(COALESCE(course_id, 'UNKNOWN')) AS course_key,
                course_id,
                access_type,
                end_at,
                CURRENT_TIMESTAMP
                ,CASE
                    WHEN end_at IS NULL THEN TRUE
                    WHEN end_at >= CURRENT_TIMESTAMP THEN TRUE
                    ELSE FALSE
                END AS is_active
            FROM normalized
            WHERE course_id IS NOT NULL AND course_id <> ''
            """,
            [str(students_path), date_key, date_key],
        )

        # Region + course aggregate built from enrollment event fact.
        conn.execute(
            "DELETE FROM mv_course_enrollment_region_daily WHERE date_key = ?",
            [date_key],
        )
        conn.execute(
            """
            INSERT INTO mv_course_enrollment_region_daily (date_key, region_name, course_key, course_name, enrolled_students_count, refreshed_at)
            SELECT
                e.date_key,
                COALESCE(dc.region_name, 'UNKNOWN') AS region_name,
                e.course_key,
                COALESCE(cr.course_name, 'UNKNOWN') AS course_name,
                COUNT(DISTINCT e.student_id) AS enrolled_students_count,
                CURRENT_TIMESTAMP
            FROM fact_course_enrollment_event e
            LEFT JOIN dim_city dc ON dc.city_key = e.city_key
            LEFT JOIN dim_course cr ON cr.course_key = e.course_key
            WHERE e.date_key = ?
            GROUP BY e.date_key, region_name, e.course_key, course_name
            """,
            [date_key],
        )

        student_total_count = conn.execute(
            "SELECT COALESCE(SUM(student_total_count), 0) FROM fact_student_daily WHERE date_key = ?",
            [date_key],
        ).fetchone()[0]
        student_paid_count = conn.execute(
            "SELECT COALESCE(SUM(student_paid_count), 0) FROM fact_student_daily WHERE date_key = ?",
            [date_key],
        ).fetchone()[0]
        avg_paid_rate = conn.execute(
            "SELECT COALESCE(AVG(paid_rate), 0.0) FROM mv_paid_students_daily WHERE date_key = ?",
            [date_key],
        ).fetchone()[0]
        student_course_enrollments = conn.execute(
            "SELECT COALESCE(COUNT(*), 0) FROM fact_course_enrollment_event WHERE date_key = ?",
            [date_key],
        ).fetchone()[0]

        payload = {
            "ds": ds,
            "date_key": date_key,
            "duckdb_file": str(DUCKDB_PATH),
            "rows": {
                "student_total_count": int(student_total_count),
                "student_paid_count": int(student_paid_count),
                "avg_paid_rate": float(avg_paid_rate),
                "student_course_enrollments": int(student_course_enrollments),
            },
            "tables": [
                "dim_date",
                "dim_city",
                "dim_student",
                "dim_course",
                "fact_student_daily",
                "mv_paid_students_daily",
                "fact_course_enrollment_event",
                "mv_course_enrollment_region_daily",
            ],
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }

        logger.info("(Processed %s records)", int(student_total_count))

        metrics_path = _metrics_path(ds)
        metrics_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        context["ti"].xcom_push(key="gold_metrics", value=payload)
        return payload
    finally:
        conn.close()


def run_bronze_ingest(collection_name: str, ds: str, **context):
    """Wrapper to call Phase 04 ingestion callable."""
    return ingest_collection_phase4(collection_name=collection_name, ds=ds, **context)


def run_silver_standardization(collection_name: str, ds: str, **context):
    """Wrapper to route to correct Phase 05 standardization callable."""
    if collection_name == "courses":
        return standardize_courses_phase5(ds=ds, **context)
    elif collection_name=="metrics":
        return 
    return standardize_collection_phase5(
        collection_name=collection_name, ds=ds, **context
    )


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
    dag_id="bronze_silver_gold_duckdb_daily",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",
    catchup=False,
    description="One DAG chaining Bronze -> Silver -> Gold (paid students KPI)",
    tags=[
        "bronze",
        "silver",
        "gold",
        "duckdb",
    ],
) as dag:
    bronze_students_ingest = PythonOperator(
        task_id="bronze_students_ingest",
        python_callable=run_bronze_ingest,
        op_kwargs={"collection_name": "students", "ds": "{{ ds }}"},
    )

    silver_students_standardize = PythonOperator(
        task_id="silver_students_standardize",
        python_callable=run_silver_standardization,
        op_kwargs={"collection_name": "students", "ds": "{{ ds }}"},
    )

    silver_courses_standardize = PythonOperator(
        task_id="silver_courses_standardize",
        python_callable=run_silver_standardization,
        op_kwargs={"collection_name": "courses", "ds": "{{ ds }}"},
    )

    silver_metrics = PythonOperator(
        task_id="silver_metrics",
        python_callable=run_silver_standardization,
        op_kwargs={"collection_name": "metrics", "ds": "{{ ds }}"},
    )
    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold_for_ds,
        op_kwargs={"ds": "{{ ds }}"},
    )

    bronze_students_ingest >> silver_students_standardize
    [silver_students_standardize, silver_courses_standardize] >>silver_metrics >> build_gold
