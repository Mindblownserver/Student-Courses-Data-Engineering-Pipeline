Project: Data Engineering — Bronze → Silver → Gold (with streaming)
=============================================

Overview
--------
This repository contains a small end-to-end data engineering project that implements a layered pipeline:

- Phase 04 — Bronze: MongoDB extraction into Parquet (date-partitioned) with metadata and watermarks
- Phase 05 — Silver: Standardization and quality gates (Pandas-based) producing clean parquet + quarantine files
- Phase 06 — Gold: OLAP modeling using DuckDB (dimensions, facts, KPI materialized tables)
- Phase 07 — Serving / Streaming: lightweight streaming producer/consumer (Kafka) and a Streamlit dashboard

The Airflow-based orchestration lives in the "Airflow Setup" area and the DAGs in their respective PHASE_* folders.

Quick Project Map
-----------------
- PHASE_04_BRONZE_STORAGE/: ingestion code (ingest_client_db_mongodb_phase4.py) and README for Bronze layering
- PHASE_05_SILVER_STANDARDIZATION/: silver standardization DAG (silver_standardization_dag.py) and utils/data_quality.py
- PHASE_06_GOLD_MODELING/: gold DAG (gold_modeling_duckdb_dag.py) and README_PHASE_06.md
- PHASE_07_SERVING_STREAMING/streaming/: producer/consumer for courses events (Kafka), Dockerfile, requirements
- PHASE_07_SERVING_STREAMING/dashboard/: Streamlit dashboard to surface KPIs (Dockerfile, requirements)
- Airflow Setup/: helper Dockerfile, requirements, and environment example (.env.example). Contains runtime paths used by DAGs (/opt/airflow/data/...)
- db/: small sample JSON files used by local/testing (students.json, courses.json, users.json)

Important Paths & Artifacts
---------------------------
- Bronze parquet path (configured in DAGs / phase code): /opt/airflow/data/bronze/{collection}/YYYY/MM/DD/*.parquet
- Bronze stream JSONL (streaming consumer): /data/bronze_stream/courses_events/YYYY/MM/DD/events.jsonl
- Silver parquet path: /opt/airflow/data/silver/{collection}/YYYY/MM/DD/*_clean_00.parquet
- Gold DuckDB file: /opt/airflow/data/gold/gold.duckdb
- Gold metrics: /opt/airflow/data/gold/metrics/YYYY/MM/DD/gold_metrics.json

Configuration (Airflow Variables & ENV)
-------------------------------------
The DAGs and ingestion scripts expect certain Airflow Variables or environment variables to be set:

- MONGO_URI (Airflow Variable): MongoDB connection string used by Phase 04 ingestion
- MONGO_DB (Airflow Variable): Mongo database name
- duckdb_path (Airflow Variable, optional): path to gold.duckdb (default: /opt/airflow/data/gold/gold.duckdb)
- bronze_path (Airflow Variable, optional): root for bronze files (default: /data/bronze in some scripts; many DAGs use /opt/airflow/data/bronze)

See Airflow Setup/.env.example for a simple example of container uid and hints for Mongo URI.

Quick Start (development)
-------------------------
These are minimal instructions to run parts of the project locally for development. The real deployment expects Docker / Airflow orchestration and a Kafka broker.

1. Airflow (DAGs)
   - Build image with Airflow requirements (Airflow Setup/airflow.Dockerfile and airflow.requirements.txt).
   - Provide Airflow Variables (MONGO_URI, MONGO_DB) and mount a shared volume at /opt/airflow/data to match DAG paths.
   - Place DAGs under the Airflow dags folder (the PHASE_*.py files are importable by the Airflow installation).

2. Running Bronze ingestion locally (without Airflow)
   - Ensure Python 3 is installed and install pandas + pymongo: pip install pandas pymongo pyarrow fastparquet
   - Set environment variables or adapt Variable.get() calls in the module. You can also call ingest_collection(...) directly from a small script.

3. Streaming producer / consumer (Kafka required)
   - Install dependencies for streaming: pip install -r PHASE_07_SERVING_STREAMING/streaming/requirements.txt
   - Start a Kafka-compatible broker (Redpanda or local Kafka). Set KAFKA_BOOTSTRAP_SERVERS env var if needed.
   - Start consumer to write bronze stream JSONL files:
     python PHASE_07_SERVING_STREAMING/streaming/consumer_courses_events.py
   - Start producer to publish courses events (modes: mongo_poll or mock):
     COURSES_API_MODE=mock python PHASE_07_SERVING_STREAMING/streaming/producer_courses_events.py

4. Dashboard (Streamlit)
   - Install requirements: pip install -r PHASE_07_SERVING_STREAMING/dashboard/requirements.txt
   - Run: streamlit run PHASE_07_SERVING_STREAMING/dashboard/kpi_dashboard.py
   - Dashboard reads DuckDB file (see README_PHASE_06.md for path /opt/airflow/data/gold/gold.duckdb)

Key Commands (examples)
-----------------------
- Run Bronze ingestion (ad-hoc):
  python -c "from PHASE_04_BRONZE_STORAGE.ingest_client_db_mongodb_phase4 import ingest_collection; print(ingest_collection('students','2026-04-29'))"
- Run Silver standardization (ad-hoc):
  python -c "from PHASE_05_SILVER_STANDARDIZATION.silver_standardization_dag import standardize_collection; print(standardize_collection('students','2026-04-29'))"
- Run Gold modeling (ad-hoc):
  python -c "from PHASE_06_GOLD_MODELING.gold_modeling_duckdb_dag import build_gold_for_ds; print(build_gold_for_ds('2026-04-29'))"

Notes, Caveats, and TODOs
------------------------
- This repository is structured as a teaching / lab project. Many paths are hard-coded to /opt/airflow/data or /data/* to match a Docker Compose layout used during development.
- The Airflow DAGs dynamically import local callables from phase modules using known candidate paths — ensure DAGs and phase modules are reachable by Airflow's Python path.
- The streaming code requires a Kafka broker and (for mongo_poll mode) a running MongoDB instance.
- DuckDB is used for Gold modeling for convenience; materialized KPI tables are implemented as persisted tables refreshed by the DAG.
- Review README_PHASE_04.md and README_PHASE_06.md for more detail on Bronze and Gold.

Where to Look Next
------------------
- PHASE_04_BRONZE_STORAGE/README_PHASE_04.md — Bronze design and operational guidance
- PHASE_06_GOLD_MODELING/README_PHASE_06.md — Gold modeling and DuckDB notes
- PHASE_05_SILVER_STANDARDIZATION/utils/data_quality.py — canonical standardization rules
- PHASE_07_SERVING_STREAMING/streaming/* — Kafka producer and consumer for course events
- Airflow Setup/* — Dockerfile and requirements used to build the Airflow image

Author
------
Repository maintained by the project author. See git history for commit metadata.
