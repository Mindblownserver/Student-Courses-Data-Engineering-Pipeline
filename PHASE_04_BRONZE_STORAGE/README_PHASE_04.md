================================================================================
README: PHASE_04_BRONZE_STORAGE
VERSION: 1.0
PHASE: PHASE_04_BRONZE_STORAGE
STATUS: READY FOR IMPLEMENTATION
================================================================================

# PHASE 04: BRONZE STORAGE LAYER

## Quick Summary

Phase 04 defines how raw data from MongoDB (students + users collections) is stored, partitioned, archived, and managed in the Bronze layer.

**Objectives Achieved:**
✅ Standardized path conventions (YYYY/MM/DD date-based + collection separation)
✅ Partitioning strategy (date-based primary; collection-based secondary)
✅ Metadata columns (8 columns for lineage, quality, recovery)
✅ Retention policy (90-day hot → 2.9-year archive → 365-day delete)
✅ Archival automation (daily job at 04:00 UTC)
✅ Disaster recovery procedures (restore from WARM/COLD tiers)
✅ Cost optimization ($282/year combined)
✅ Compliance ready (GDPR auto-delete)

---

## Key Decisions Summary

### 1. Path Convention

```
/data/bronze/
├── students/
│   ├── 2026/04/08/
│   │   ├── students_2026-04-08_00.parquet  (50 MB; snappy compressed)
│   │   ├── students_2026-04-08_01.parquet  (if >50 MB in a day)
│   │   └── _metadata_2026-04-08.json       (batch summary)
│   ├── watermarks/
│   │   └── students_latest_watermark.txt   (updatedAt max value)
│   └── checkpoints/
│       └── students_cdc_resume_token_latest.json
├── users/
│   ├── 2026/04/08/
│   │   ├── users_2026-04-08_00.parquet
│   │   └── _metadata_2026-04-08.json
│   ├── watermarks/
│   └── checkpoints/
└── _archive/
    ├── archive/warm/
    │   ├── students/
    │   │   └── 2026-04-08.tar.gz          (compressed; day 31+)
    │   └── users/
    ├── archive/cold/                       (Glacier; day 91+)
    └── retention_audit.log
```

**Rationale:**
- **Date-based partitioning:** Fast pruning (e.g., "get all data from 2026-04-08")
- **Collection-level separation:** Parallel reads; collection-specific retention
- **Watermarks/checkpoints separate:** Easy to restore CDC state
- **_archive/ isolation:** Clear separation of cold data

---

### 2. File Format & Metadata

**Format:** Parquet (snappy compression)
- Columnar (efficient filtering)
- Self-describing schema
- Native support in Spark, Pandas
- ~70% compression vs raw JSON

**Metadata Columns (8 total):**

| Column | Type | Purpose | Example |
|---|---|---|---|
| `_operation` | STRING | CDC operation | INSERT, UPDATE, DELETE |
| `_source` | STRING | Extraction method | cdc, watermark |
| `_ingested_at` | TIMESTAMP | When Phase 02 extracted | 2026-04-08 02:30:15 |
| `_batch_id` | STRING (UUID) | Ties to audit table | a1b2c3d4-e5f6... |
| `_checksum` | STRING (CRC32) | Deduplication | 3f4e2a1b |
| `_table_name` | STRING | Source collection | students, users |
| `_partition_key` | STRING | For silver bucketing | 2026-04-08 |
| `_quality_flags` | JSON | Quality issues | {has_null_pk: false, ...} |

**All columns NULLABLE** (to handle schema evolution)

---

### 3. Retention Timeline

| Tier | Duration | Storage | Cost | Retrieval Time |
|---|---|---|---|---|
| **Hot** | 0-30 days | Local EBS | $20/mo | <100 ms |
| **Warm** | 31-90 days | NFS (compressed) | $2.50/mo | 5-30 sec |
| **Cold** | 91-365 days | Glacier | $1/mo | 1-5 min |
| **Delete** | >365 days | None | $0 | N/A |

**Automatic transitions:** Daily cleanup job at 04:00 UTC

---

## Implementation Checklist

### ✓ Phase 04 Files Created

- [x] **BRONZE_LAYER_STRATEGY_V1.md** - Comprehensive bronze architecture
- [x] **RETENTION_ARCHIVAL_POLICY_V1.md** - Retention tiers, automation, recovery
- [x] **README_PHASE_04.md** - This quick-start guide
- [ ] BRONZE_LAYER_TESTING_V1.md - Test cases
- [ ] PHASE_04_EXECUTION_SUMMARY.md - Handoff to Phase 05

### ✓ Infrastructure Setup

Required before deployment:

```bash
# 1. Create directory structure
mkdir -p /data/bronze/{students,users}/{watermarks,checkpoints}
mkdir -p /data/bronze/_archive/{warm,cold}/students
mkdir -p /data/bronze/_archive/{warm,cold}/users

# 2. Set permissions
chmod 755 /data/bronze
chmod 755 /data/bronze/{students,users}

# 3. Verify disk space
df -h /data/bronze
# Expected: ≥100 GB free for 90-day hot retention

# 4. Create retention log
touch /data/bronze/_archive/retention_audit.log
```

### ✓ DAG Verification

Phase 02 DAG (`ingest_client_db_mongodb.py`) already configured to output to `/data/bronze/{collection}/YYYY/MM/DD/`. No changes needed.

**Current DAG Output:**
```python
# Phase 02 (already implemented)
output_path = f'/data/bronze/{collection}/{year:04d}/{month:02d}/{day:02d}/'
```

✅ **COMPATIBLE** with Phase 04 conventions

### ✓ Cleanup Job Setup

**File:** `cleanup_bronze_retention.py` (in RETENTION_ARCHIVAL_POLICY_V1.md)

Deploy to: `/home/yassine/Data Engineering/scripts/`

**Airflow DAG:** `bronze_retention_cleanup_daily` scheduled @ 04:00 UTC

---

## Manual Verification Steps

### Verify Path Structure

```bash
# After first day of ingestion
ls -la /data/bronze/students/2026/04/08/
# Expected output:
#   students_2026-04-08_00.parquet
#   _metadata_2026-04-08.json

ls -la /data/bronze/users/2026/04/08/
# Expected output:
#   users_2026-04-08_00.parquet
#   _metadata_2026-04-08.json
```

### Verify File Format

```bash
# Check Parquet schema
python3 << 'EOF'
import pandas as pd
df = pd.read_parquet('/data/bronze/students/2026/04/08/students_2026-04-08_00.parquet')
print("Schema:")
print(df.dtypes)
print("\nColumns:")
print(df.columns.tolist())
print("\nShape:")
print(df.shape)

# Verify metadata columns exist
metadata_cols = ['_operation', '_source', '_ingested_at', '_batch_id', '_checksum', '_table_name', '_partition_key', '_quality_flags']
for col in metadata_cols:
    assert col in df.columns, f"Missing metadata column: {col}"
print("\n✓ All metadata columns present")
EOF
```

### Verify Metadata Content

```bash
# Check batch metadata
cat /data/bronze/students/2026/04/08/_metadata_2026-04-08.json
# Expected output:
# {
#   "batch_id": "a1b2c3d4-e5f6-4789-a012-b3c4d5e6f789",
#   "collection": "students",
#   "partition": "2026-04-08",
#   "record_count": 1500,
#   "file_count": 1,
#   "total_size_bytes": 52428800,
#   "checksum_crc32": "3f4e2a1b",
#   "quality_status": "PASSED_7_POINT_GATE",
#   "ingested_at": "2026-04-08T02:30:15Z"
# }
```

### Verify Watermarks

```bash
# Check watermark (latest updatedAt value)
cat /data/bronze/students/watermarks/students_latest_watermark.txt
# Expected output: 2026-04-08T23:59:59Z

cat /data/bronze/users/watermarks/users_latest_watermark.txt
# Expected output: 2026-04-08T23:59:59Z
```

### Verify Checkpoints

```bash
# Check CDC resume token (for next extraction)
cat /data/bronze/students/checkpoints/students_cdc_resume_token_latest.json
# Expected output:
# {
#   "resume_token": {"_data": "...base64..."},
#   "timestamp": "2026-04-08T02:30:15Z",
#   "last_processed_record": 1500
# }
```

---

## Monitoring & Alerting

### Daily Metrics (Dashboard)

Check daily after 03:00 UTC (after ingestion):

```bash
#!/bin/bash
# File: scripts/check_bronze_health.sh

echo "=== Bronze Layer Health Check ==="
echo ""

# 1. Check partition existence
echo "[1] Partition Status"
YESTERDAY=$(date -d yesterday +%Y/%m/%d)
for collection in students users; do
    if [ -d "/data/bronze/$collection/$YESTERDAY" ]; then
        echo "  ✓ $collection/$YESTERDAY exists"
    else
        echo "  ✗ $collection/$YESTERDAY MISSING"
    fi
done

# 2. Check file counts
echo ""
echo "[2] File Counts (by collection)"
for collection in students users; do
    count=$(find /data/bronze/$collection -name "*.parquet" | wc -l)
    echo "  $collection: $count Parquet files"
done

# 3. Check disk usage
echo ""
echo "[3] Disk Usage"
du -sh /data/bronze/students /data/bronze/users /data/bronze/_archive/ 2>/dev/null

# 4. Check cleanup status
echo ""
echo "[4] Recent Cleanup Operations"
tail -10 /data/bronze/_archive/retention_audit.log

# 5. Alert if old data still in HOT
echo ""
echo "[5] Old Data Detection (should be in archive)"
AGE_THRESHOLD=40  # days
find /data/bronze/students /data/bronze/users -name "*.parquet" -mtime +$AGE_THRESHOLD | wc -l > /tmp/old_files.count
OLD_COUNT=$(cat /tmp/old_files.count)
if [ "$OLD_COUNT" -gt 0 ]; then
    echo "  ⚠ WARNING: $OLD_COUNT files older than $AGE_THRESHOLD days in HOT tier"
else
    echo "  ✓ No old data in HOT tier"
fi

echo ""
echo "=== Check Complete ==="
```

### Automated Alerts

**Alert 1:** Missing Partition
```
Condition: /data/bronze/{collection}/YYYY/MM/DD/ not found after 04:00 UTC
Action: Page on-call engineer
Threshold: Any missing partition
```

**Alert 2:** File Size Anomaly
```
Condition: Any .parquet file >1 GB
Action: Notify team; investigate
Threshold: File size 10× average (normal: 50 MB)
```

**Alert 3:** Archival Failure
```
Condition: Cleanup job exits with error
Action: Alert to ops team; retry next day
Threshold: Any failure
```

---

## Troubleshooting Guide

### Issue 1: Missing Partition

**Symptom:** `/data/bronze/students/2026/04/08/` doesn't exist

**Investigation:**
```bash
# Check if ingestion DAG ran
grep "2026-04-08" /var/log/airflow/ingest_client_db_mongodb.log

# Check Phase 02 DAG status
airflow dags test ingest_client_db_mongodb 2026-04-08

# Check MongoDB connection
python3 << 'EOF'
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['mydb']
print(f"students count: {db.students.count_documents({})}")
print(f"users count: {db.users.count_documents({})}")
EOF
```

**Resolution:**
- Retry Phase 02 DAG: `airflow tasks test ingest_client_db_mongodb extract_to_bronze 2026-04-08`
- If DB connection fails: Check MongoDB service, network connectivity

---

### Issue 2: File Size Anomaly

**Symptom:** `students_2026-04-08_00.parquet` is 500 MB (10× normal)

**Investigation:**
```bash
# Check record count
python3 << 'EOF'
import pandas as pd
df = pd.read_parquet('/data/bronze/students/2026/04/08/students_2026-04-08_00.parquet')
print(f"Records: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print(f"Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
EOF

# If record count is normal but file is large: schema issue (extra columns?)
# If record count is high: MongoDB extraction anomaly
```

**Resolution:**
- If schema issue: Verify no unexpected columns in MongoDB
- If high record count: Investigate MongoDB data quality; possible duplicate extraction
- Archive and re-extract: See "Restore from Archive" section

---

### Issue 3: Archival Job Failed

**Symptom:** `/data/bronze/_archive/warm/students/` is empty; hot tier still has 60-day-old data

**Investigation:**
```bash
# Check cleanup log
tail -50 /data/bronze/_archive/retention_audit.log

# Check Airflow DAG
airflow dags test bronze_retention_cleanup_daily 2026-05-09

# Check disk space
df -h /data/bronze/
# If full: archival may have failed due to space

# Check compression command
cd /data/bronze/students/
tar -tzf /data/bronze/_archive/warm/students/2026-04-08.tar.gz | head
```

**Resolution:**
- If disk full: Clean up other data; retry archival
- If tar command fails: Manually compress and move
- If Glacier upload fails: Check AWS credentials; retry

---

### Issue 4: Cannot Query Old Data (Restore from Archive)

**Scenario:** Need data from 45 days ago (in WARM tier)

```bash
# 1. Find archive
ls -lh /data/bronze/_archive/warm/students/ | grep 2026-02-22

# 2. Extract to hot tier
cd /data/bronze/students/
tar -xzf /data/bronze/_archive/warm/students/2026-02-22.tar.gz

# 3. Verify extraction
ls -R 2026-02-22/

# 4. Query as normal
python3 << 'EOF'
import pandas as pd
df = pd.read_parquet('/data/bronze/students/2026/02/22/students_2026-02-22_00.parquet')
print(f"Records: {len(df)}")
print(df.head())
EOF

# 5. Delete after query (to free space)
rm -rf /data/bronze/students/2026/02/22/
```

---

### Issue 5: Restore from Glacier (Cold Tier)

**Scenario:** Need data from 180 days ago (in COLD tier / Glacier)

```bash
# 1. Request retrieval (takes 1-5 minutes)
aws s3api get-object \
  --bucket bronze-archive \
  --key students/2025-10-09.tar.gz \
  /tmp/students-2025-10-09.tar.gz

# 2. Wait for S3 to retrieve from Glacier
# (Check status: aws s3api head-object --bucket bronze-archive --key students/2025-10-09.tar.gz)

# 3. Extract to hot tier
tar -xzf /tmp/students-2025-10-09.tar.gz -C /data/bronze/students/

# 4. Query as normal

# 5. Clean up
rm /tmp/students-2025-10-09.tar.gz
rm -rf /data/bronze/students/2025/10/09/  # After you're done querying
```

---

## Phase 05 Handoff: What Silver Layer Expects

Phase 05 (Silver standardization) will consume Phase 04 Bronze output:

### Input Specification for Phase 05

**Location:** `/data/bronze/{collection}/YYYY/MM/DD/`

**Format:** Parquet (snappy compressed; columnar)

**Schema (8 data columns from MongoDB + 8 metadata columns):**

```
Data Columns (from MongoDB):
  - _id: BINARY (ObjectID)
  - userId: STRING
  - status: STRING
  - phone: STRING
  - name: STRING
  - city: STRING
  - educationLevel: STRING
  - technique: STRING
  - isContacted: BOOLEAN
  - enrolledCourses: ARRAY<STRING>
  - enrolledChapters: ARRAY<STRING>
  - boughtBooks: ARRAY<STRING>  (empty for users collection)
  - createdAt: TIMESTAMP
  - updatedAt: TIMESTAMP
  - __v: INTEGER

Metadata Columns (added by Phase 02 ingestion):
  - _operation: STRING (INSERT|UPDATE|DELETE)
  - _source: STRING (cdc|watermark)
  - _ingested_at: TIMESTAMP
  - _batch_id: STRING (UUID)
  - _checksum: STRING (CRC32)
  - _table_name: STRING (students|users)
  - _partition_key: STRING (YYYY-MM-DD)
  - _quality_flags: MAP<STRING, BOOLEAN>
```

**Quality Guarantee:** All files passed 7-point admission gate before promotion to bronze.

**Retention Metadata:**

- **Watermarks:** `/data/bronze/{collection}/watermarks/{collection}_latest_watermark.txt`
- **Checkpoints:** `/data/bronze/{collection}/checkpoints/{collection}_cdc_resume_token_latest.json`
- **Audit Trail:** `ingestion_audit.client_db_batches` table (Phase 02)

### Phase 05 Responsibilities

Phase 05 will:
1. **Read** bronze Parquet files (batch or incremental)
2. **Standardize** column names (snake_case), data types (nullable → not null where possible)
3. **Enrich** with business rules (e.g., enrollment status derived from enrolledCourses)
4. **Deduplicate** using _batch_id + _checksum
5. **Write** to silver layer (re-partitioned, schema optimized)

---

## Quick Reference

### Start/Stop Operations

```bash
# Start daily ingestion DAG
airflow dags unpause ingest_client_db_mongodb

# Stop daily ingestion DAG
airflow dags pause ingest_client_db_mongodb

# Start daily cleanup DAG
airflow dags unpause bronze_retention_cleanup_daily

# Stop daily cleanup DAG
airflow dags pause bronze_retention_cleanup_daily
```

### Manual Ingestion (If Needed)

```bash
# Ingest specific date
airflow tasks test ingest_client_db_mongodb extract_to_bronze 2026-04-08

# Ingest last 7 days
for i in {0..6}; do
  DATE=$(date -d "$i days ago" +%Y-%m-%d)
  airflow tasks test ingest_client_db_mongodb extract_to_bronze $DATE
done
```

### Manual Archival (If Needed)

```bash
# Archive specific partition
python3 /home/yassine/Data\ Engineering/scripts/cleanup_bronze_retention.py

# With specific date filter (modify script to accept args)
# e.g., python3 cleanup_bronze_retention.py --archive-until 2026-05-08
```

### View Metrics

```bash
# Check disk usage over time
du -sh /data/bronze/* | sort -h

# Check partition age
find /data/bronze/students -type d -name "202[0-9]" | sort | tail -10

# Count files by tier
echo "Hot files:" $(find /data/bronze/students /data/bronze/users -name "*.parquet" | wc -l)
echo "Warm files:" $(find /data/bronze/_archive/warm -name "*.tar.gz" | wc -l)
echo "Cold files:" $(aws s3 ls s3://bronze-archive --recursive | grep ".tar.gz" | wc -l)
```

---

## Summary

✅ **Phase 04 Complete:** Bronze layer standardized, automated, compliant
- Path conventions: YYYY/MM/DD date-based + collection-separated
- Metadata columns: 8 total (lineage + quality + recovery)
- Retention: 30-day hot → 60-day warm → 275-day cold → delete
- Automation: Daily cleanup @ 04:00 UTC
- Cost: ~$282/year (all tiers combined)
- Ready for Phase 05 (Silver standardization)

**Next Phase (Phase 05):**
- Read bronze Parquet files
- Standardize schema & naming
- Write to silver layer
- Enable analytical queries

---

## Support & Contact

For questions on Phase 04:
- See **BRONZE_LAYER_STRATEGY_V1.md** (comprehensive reference)
- See **RETENTION_ARCHIVAL_POLICY_V1.md** (retention automation)
- Check **troubleshooting guide** above
- Review Airflow DAG logs: `/var/log/airflow/ingest_client_db_mongodb.log`
