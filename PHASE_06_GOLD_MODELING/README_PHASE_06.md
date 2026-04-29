# PHASE 06 - Gold Modeling (DuckDB)

Phase 06 builds OLAP-ready Gold artifacts from Silver clean data using DuckDB.

## Gold model

- Dimensions:
  - `dim_date`
  - `dim_city` (with derived `region_name`)
- Facts:
  - `fact_user_daily`
  - `fact_student_daily`
- Materialized KPI table:
  - `mv_conversion_daily` (materialized conversion rate)

## Storage

- Database file: `/opt/airflow/data/gold/gold.duckdb`
- Metrics output: `/opt/airflow/data/gold/metrics/YYYY/MM/DD/gold_metrics.json`

## Docker communication setup (Airflow <-> DuckDB)

- Compose includes a `duckdb-service` container mounted on the same shared volume:
  - Host: `PHASE 4 EXEC/data`
  - Airflow containers path: `/opt/airflow/data`
  - DuckDB container path: `/data`
- Airflow writes the DB file using:
  - `AIRFLOW_VAR_DUCKDB_PATH=/opt/airflow/data/gold/gold.duckdb`
- DuckDB container can inspect the same file at:
  - `/data/gold/gold.duckdb`

Example inspection from the DuckDB container:

```bash
docker exec -it de_duckdb duckdb /data/gold/gold.duckdb
```

Then in DuckDB CLI:

```sql
.tables
SELECT * FROM mv_conversion_daily ORDER BY date_key DESC LIMIT 20;
```

## Notes

- DuckDB does not provide native persisted materialized views like warehouse engines.
- `mv_conversion_daily` is implemented as a persisted table refreshed by the DAG.
