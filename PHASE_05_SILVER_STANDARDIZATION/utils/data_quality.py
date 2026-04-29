from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import uuid
from typing import Tuple

import pandas as pd


def normalize_phone(value: object) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"[^0-9]", "", str(value))
    return cleaned or None


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    lowered = str(value).strip().lower()
    return lowered in {"1", "true", "yes", "y"}


def _safe_ts(series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _col_or_default(df: pd.DataFrame, col_name: str, default_value=None):
    if col_name in df.columns:
        return df[col_name]
    return pd.Series([default_value] * len(df), index=df.index)


def _normalize_nested_array(value: object) -> list:
    """Normalize nested payloads to list values for parquet-friendly schema."""
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") or text.startswith("{"):
            try:
                parsed = json.loads(text)
            except Exception:
                return []
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
        return []
    return []


def standardize_columns(df: pd.DataFrame, collection: str) -> pd.DataFrame:
    out = df.copy()  # To ensure that base df is immutable
    ## Some columns have missing values
    ## so, we provide default_value if need be.
    out["entity_id"] = _col_or_default(out, "_id").astype("string")
    out["account_id"] = _col_or_default(out, "userId").astype("string")
    out["full_name"] = (
        _col_or_default(out, "name").astype("string").str.strip().str.title()
    )
    out["phone_number"] = _col_or_default(out, "phone").map(normalize_phone)
    out["city"] = (
        _col_or_default(out, "city").astype("string").str.capitalize().str.strip()
    )
    out["education_level"] = _col_or_default(out, "educationLevel").astype("string")
    out["account_status"] = (
        _col_or_default(out, "status").astype("string").str.lower().str.strip()
    )
    out["technique"] = _col_or_default(out, "technique").astype("string")
    out["is_contacted"] = _col_or_default(out, "isContacted").map(to_bool)

    # Keep nested fields as structured arrays (not JSON strings).
    for col in ["enrolledCourses", "enrolledChapters", "boughtBooks"]:
        if col not in out.columns:
            out[col] = [[] for _ in range(len(out))]
        out[col] = out[col].map(_normalize_nested_array)

    out["enrolled_courses"] = out["enrolledCourses"]
    out["enrolled_chapters"] = out["enrolledChapters"]
    out["bought_books"] = out["boughtBooks"]
    out["enrolled_courses_count"] = _col_or_default(out, "enrolledCourses").map(
        lambda v: len(v) if isinstance(v, list) else 0
    )
    out["is_paid_student"] = out["enrolled_courses_count"].fillna(0).astype("int64") > 0

    ingested_at = _safe_ts(_col_or_default(out, "_ingested_at"))
    out["created_at"] = _safe_ts(_col_or_default(out, "createdAt")).fillna(ingested_at)
    out["updated_at"] = (
        _safe_ts(_col_or_default(out, "updatedAt"))
        .fillna(out["created_at"])
        .fillna(ingested_at)
    )

    out["bronze_batch_id"] = _col_or_default(out, "_batch_id").astype("string")
    out["silver_batch_id"] = str(uuid.uuid4())
    out["processed_at"] = datetime.now(timezone.utc)
    out["_table_name"] = collection

    return out


def split_clean_quarantine(
    df: pd.DataFrame, bronze_file_path: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """It splits records into two: clean records and ones that need to be isolated (quarantined) because they can crash the pipeline"""
    checks = pd.DataFrame(index=df.index)
    checks["missing_pk"] = df["entity_id"].isna() | (
        df["entity_id"].astype("string").str.strip() == ""
    )
    checks["bad_created_at"] = df["created_at"].isna()
    checks["bad_updated_at"] = df["updated_at"].isna()

    hard_fail = checks.any(axis=1)
    quarantine = df.loc[hard_fail].copy()
    if not quarantine.empty:
        quarantine["raw_payload"] = quarantine.to_dict(orient="records")
        quarantine["raw_payload"] = quarantine["raw_payload"].map(
            lambda x: json.dumps(x, default=str, ensure_ascii=True)
        )
        quarantine["quarantine_reason"] = checks.loc[hard_fail].apply(
            lambda row: ",".join([k for k, v in row.items() if bool(v)]), axis=1
        )
        quarantine["source_table"] = quarantine["_table_name"]
        quarantine["bronze_file_path"] = bronze_file_path
        quarantine["quarantined_at"] = datetime.now(timezone.utc)
        quarantine = quarantine[
            [
                "raw_payload",
                "quarantine_reason",
                "source_table",
                "bronze_file_path",
                "quarantined_at",
            ]
        ]

    clean = df.loc[~hard_fail].copy()
    if "_ingested_at" not in clean.columns:
        clean["_ingested_at"] = pd.NaT
    clean = clean.sort_values(
        ["entity_id", "updated_at", "_ingested_at"], ascending=[True, False, False]
    )
    clean = clean.drop_duplicates(subset=["entity_id"], keep="first")

    selected = [
        "entity_id",
        "account_id",
        "full_name",
        "phone_number",
        "city",
        "education_level",
        "account_status",
        "technique",
        "is_contacted",
        "enrolled_courses",
        "enrolled_courses_count",
        "is_paid_student",
        "enrolled_chapters",
        "bought_books",
        "created_at",
        "updated_at",
        "bronze_batch_id",
        "silver_batch_id",
        "processed_at",
        "_operation",
        "_source",
        "_table_name",
        "_partition_key",
        "_checksum",
    ]
    present = [c for c in selected if c in clean.columns]
    clean = clean[present]

    return clean, quarantine
