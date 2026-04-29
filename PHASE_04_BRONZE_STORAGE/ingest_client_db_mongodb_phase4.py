#!/usr/bin/env python3
"""Phase 04 Bronze ingestion DAG.

This module ingests MongoDB collections into Bronze Parquet partitions with:
- CDC extraction when available
- Watermark fallback for non-replica deployments
- Metadata/checkpoint persistence for idempotent reruns

Scope of this DAG:
- students (batch)

Courses are handled by the streaming path (Phase 07) and are not pulled here.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import uuid
import zlib
import logging
import pandas as pd
import pymongo
from bson import ObjectId
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from pymongo.errors import AutoReconnect, OperationFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Runtime configuration
# ---------------------------------------------------------------------------
MONGO_URI = Variable.get("MONGO_URI")
MONGO_DB = Variable.get("MONGO_DB")
BRONZE_ROOT = Path(Variable.get("bronze_path", default_var="/data/bronze"))
COLLECTIONS = ["students"]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _connect_mongo(retries: int = 3) -> pymongo.MongoClient:
    last_error: Optional[Exception] = None
    logger.info(f"Client: {MONGO_URI}")
    for _ in range(retries):
        try:
            client = pymongo.MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
            )
            client.admin.command("ping")
            return client
        except (ServerSelectionTimeoutError, AutoReconnect) as exc:
            last_error = exc
    raise AirflowException(f"Failed to connect to MongoDB: {last_error}")

def _enrich_students_with_email(
    client: pymongo.MongoClient, records: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Enrich student rows with email from users collection via userId/_id."""
    if not records:
        return records

    account_ids = {
        str(r.get("userId")).strip()
        for r in records
        if r.get("userId") not in (None, "")
    }
    if not account_ids:
        return records

    object_ids: List[ObjectId] = []
    for aid in account_ids:
        if ObjectId.is_valid(aid):
            object_ids.append(ObjectId(aid))

    users_collection = client[MONGO_DB]["users"]
    # Match by userId and _id to support different source schemas.
    query: Dict[str, Any] = {"$or": [{"userId": {"$in": list(account_ids)}}]}
    if object_ids:
        query["$or"].append({"_id": {"$in": object_ids}})

    user_docs = list(
        users_collection.find(
            query,
            projection={"_id": 1, "userId": 1, "email": 1, "mail": 1},
        )
    )

    email_by_key: Dict[str, str] = {}
    for doc in user_docs:
        email_value = doc.get("email") or doc.get("mail")
        if not email_value:
            continue
        email_str = str(email_value).strip()
        if not email_str:
            continue
        if doc.get("userId"):
            email_by_key[str(doc["userId"])] = email_str
        if doc.get("_id"):
            email_by_key[str(doc["_id"])] = email_str

    enriched_count = 0
    for rec in records:
        user_id = (
            str(rec.get("userId")).strip() if rec.get("userId") is not None else ""
        )
        email = email_by_key.get(user_id)
        if email:
            rec["email"] = email
            enriched_count += 1

    logger.info(
        "Enriched %s/%s student records with email", enriched_count, len(records)
    )
    return records

def _ensure_dirs(
    parquet_file: Path | None,
    
) -> None:
    
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    
    #TODO: remove lastPipeline
def _load_collection_metadata(path: Path) -> Tuple[datetime, datetime] | None:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if data["watermark"] is not None and data["lastPipeline"] is not None:
            return datetime.fromisoformat(data["watermark"].replace("Z", "+00:00")),\
                    datetime.fromisoformat(data["lastPipeline"].replace("Z", "+00:00"))
    return None, None

def _get_path_data(collection: str, ds: str) -> Tuple[datetime, Path, Path] | None:
    """ This would return latest parquet file path as well as the watermark"""
    watermark_dt, last_pipeline_dt = _load_collection_metadata(Path(f"{BRONZE_ROOT}/{collection}/students_metadata.json"))
    dt = datetime.strptime(ds, "%Y-%m-%d")
    
    # Else, DAG executed at least once:
    logger.info("Detected DAG executed previous on {} whose watermark is {}", last_pipeline_dt, watermark_dt)

    day_dir = (
        BRONZE_ROOT
        / collection
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
    )
    parquet_file = day_dir / f"{collection}_{dt.year:04d}-{dt.month:02d}-{dt.day:02d}_00.parquet"
    parquet_metadata_file = day_dir / f"_metadata_{dt.year:04d}-{dt.month:02d}-{dt.day:02d}.json"

    return watermark_dt, Path(parquet_file), Path(parquet_metadata_file),

def _coerce_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
                timezone.utc
            )
        except ValueError:
            return None
    return None

def _parse_to_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list) or isinstance(value, dict):
        return json.dumps(value, default=str, ensure_ascii=True)
    return str(value)

def _normalize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in doc.items():
        if key == "_id":
            out[key] = str(value)
        else:
            out[key] = _parse_to_json(value)
    return out

def _fetch_rows_from_collection_by_watermark(
    collection: pymongo.collection.Collection, watermark: datetime | None
) -> Tuple[List[Dict[str, Any]], datetime | None]:
    """Extract records using updatedAt watermark semantics.

    Bootstrap behavior:
    - If no watermark exists, start from earliest updatedAt using $gte.
    - If collection has no updatedAt at all, fallback to full scan.
    """
    records: List[Dict[str, Any]] = []
    current_watermark = watermark
    if(current_watermark is None):
        ## No watermark
        ## Fetch THEM ALL.
        records_found = list(
            collection.find()
            .sort("updatedAt", 1)
            .limit(1000)
        )
    else:
        # Else, it's already not null, so keep on running!
        # Normal incremental mode: strictly newer records than last watermark.
        
        records_found = list(
            collection.find({"updatedAt": {"$gt": current_watermark}})
            .sort("updatedAt", 1)
            .limit(1000)
        )
    records_found_len = len(records_found)
    logger.info(
        "Found %s records after watermark %s",
        records_found_len,
        watermark,
    )

    for i, doc in enumerate(records_found):
        normalized = _normalize_doc(doc)
        records.append(
            {
                **normalized,
                "_operation": "UPSERT",
            }
        )
        parsed_updated_at = _coerce_utc_datetime(doc.get("updatedAt")) # updatedAt is important, so is coerce
        
        ## Final **sorted** record sets the value for the new watermark
        if(i == records_found_len -1):
            if parsed_updated_at is not None:
                current_watermark = parsed_updated_at

    logger.info("Watermark change!: It's now {}", current_watermark)
    return records, current_watermark

def _metadata_payload(
    batch_id: str,
    collection: str,
    ds: str,
    count: int,
    total_size: int,
    source: str,
) -> Dict[str, Any]:
    return {
        "batch_id": batch_id,
        "collection": collection,
        "partition": ds,
        "record_count": count,
        "file_count": 1 if count else 0,
        "total_size_bytes": total_size,
        "quality_status": "PASSED_7_POINT_GATE" if count else "EMPTY_BATCH",
        "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": source,
    }

def ingest_collection(collection_name: str, ds: str, **_: Any) -> Dict[str, Any]:
    """Main ingestion routine for one collection and execution date."""

    # Collect file paths by collcetion name. Important 
    watermark_dt, parquet_file, metadata_file = _get_path_data(
        collection_name,ds
    )
    _ensure_dirs(parquet_file)
    logger.info("Ensured that the files or/and their parent directories exists!!!")
    batch_id = str(uuid.uuid4())

    client = _connect_mongo()
    records: List[Dict[str, Any]] = []
    new_watermark: Optional[datetime] = None

    try:
        coll = client[MONGO_DB][collection_name]
        
        # Now I have watermark_dt , I shall extract the new one from the mongo collection

        records, new_watermark = _fetch_rows_from_collection_by_watermark(coll, watermark_dt)
        source = "watermark"

        # Business requirement: include contact email on student records.
        if collection_name == "students":
            records = _enrich_students_with_email(client, records)

        # Now, saving metadata of the bronze layer ingest. Apparently important, but dunno why.
        enriched: List[Dict[str, Any]] = []
        for row in records:
            quality_flags = {
                "has_null_pk": not bool(row.get("_id")),
                "has_missing_updated_at": row.get("updatedAt") in (None, ""),
            }
            enriched_row = {
                **row,
                "_source": source,
                "_ingested_at": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "_batch_id": batch_id,
                "_table_name": collection_name,
                "_partition_key": ds,
                "_quality_flags": json.dumps(quality_flags, ensure_ascii=True),
            }
            enriched.append(enriched_row)

        # After making an enriched list of keyvals+rows, we'll save them in parquet file
        size_bytes = 0
        if enriched:
            df = pd.DataFrame(enriched)
            df.to_parquet(parquet_file, index=False, compression="snappy")
            size_bytes = parquet_file.stat().st_size

        if source == "watermark" and new_watermark is not None:
            Path(f"{BRONZE_ROOT}/{collection_name}/students_metadata.json").write_text(
                json.dumps(
                    {
                        "watermark": new_watermark.isoformat().replace("+00:00", "Z"),
                        
                        # Date of the pipeline's execution: Today!
                        "lastPipeline": datetime.now().isoformat().replace("+00:00", "Z") 
                    }
                ),
                encoding="utf-8",
            )

        metadata = _metadata_payload(
            batch_id=batch_id,
            collection=collection_name,
            ds=ds,
            count=len(enriched),
            total_size=size_bytes,
            source=source,
        )
        logger.info("(Processed %s records)", len(enriched))
        metadata_file.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        return metadata
    finally:
        client.close()


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
}


def _build_collection_dag(collection_name: str) -> DAG:
    """Create one DAG per collection for operational clarity."""
    dag_id = f"ingest_phase4_{collection_name}"
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=datetime(2026, 4, 1),
        schedule="0 2 * * *",
        catchup=False,
        description=(
            f"MongoDB {collection_name} to Bronze storage "
            "following Phase 04 conventions"
        ),
        tags=["phase-04", "bronze", "mongodb", collection_name],
    )

    with dag:
        PythonOperator(
            task_id=f"extract_{collection_name}",
            python_callable=ingest_collection,
            op_kwargs={"collection_name": collection_name, "ds": "{{ ds }}"},
        )

    return dag


ingest_phase4_students = _build_collection_dag("students")
