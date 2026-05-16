# Pipeline Execution Guide — Caltrans PeMS Traffic Analytics

How to register for Caltrans PeMS, load Station Hour exports into Snowflake, and run the pipeline either manually or via Apache Airflow.

---

## Table of contents

1. [Prerequisites](#prerequisites)
2. [Register for Caltrans PeMS](#register-for-caltrans-pems)
3. [Download Station Hour data](#download-station-hour-data)
4. [Upload files to the Snowflake stage](#upload-files-to-the-snowflake-stage)
5. [Execute the pipeline manually](#execute-the-pipeline-manually)
6. [Execute with Apache Airflow](#execute-with-apache-airflow)
7. [Connect Tableau](#connect-tableau)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

- **Caltrans PeMS account** — `pems.dot.ca.gov` (free; manually approved by Caltrans, typically 1–3 business days)
- **Snowflake account** — trial is fine: `signup.snowflake.com`
- **Airflow 3.x + Snowflake provider** — see project `requirements*.txt`
- (Optional) **Tableau Desktop** with the Snowflake connector

---

## Register for Caltrans PeMS

1. Go to `https://pems.dot.ca.gov`.
2. Click **Register** (top-right). Fill in name, affiliation (SDSU is fine), and a brief use case ("academic capstone — traffic delay analytics").
3. Caltrans emails account approval within a few business days. **Plan around this lead time.**
4. After approval, sign in and confirm you can see the **Data Clearinghouse** tab in the top nav.

---

## Download Station Hour data

1. **Data Clearinghouse → Type = "Station Hour" → District = (all 12 or one at a time)**.
2. Pick a year/month. Each file is a gzipped CSV like `d04_text_station_hour_2024_01.txt.gz`.
3. Download all months 2022 → 2024 (statewide × hourly × 3 years ≈ ~430 files, ~40 GB compressed). Recommend scripting the downloads — Caltrans does not offer a bulk button.
4. Also grab **Type = "Meta" → Station Metadata** for the same districts: `d04_text_meta_2024_01_06.txt`. This is the SCD source for `dim_station`.

> **Tip:** Start with a single district-month (e.g. D11 January 2024) to validate end-to-end before pulling the full statewide history.

---

## Upload files to the Snowflake stage

After running `sql/01_setup.sql` to create `@TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES`:

**Option A — Snowsight UI:** Databases → `TRAFFIC_PEMS_DB` → `STAGING` → Stages → `STG_PEMS_FILES` → **Upload**. Good for ad-hoc testing.

**Option B — Snowflake CLI / SnowSQL (recommended for bulk):**
```bash
snow stage copy ./pems_downloads/ @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES --recursive
# Or with snowsql:
PUT file://pems_downloads/*.gz @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES AUTO_COMPRESS=FALSE PARALLEL=8;
```

Station metadata files go to `@TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES` (header row format).

---

## Execute the pipeline manually

### Phase 1 — One-time setup (per environment)

Run in this order in Snowsight or SnowSQL. All scripts are idempotent.

| # | Script | Description |
|---|--------|-------------|
| 1 | `sql/01_setup.sql` | Database, schemas, warehouse, file formats, stages |
| 2 | `sql/02_staging.sql` | Staging tables (`stg_pems_hour_raw/deduped`, `stg_pems_station_meta_raw`) |
| 3 | `sql/02_dimensions_scd2.sql` | Dimensions (SCD2 `dim_station` + `dim_freeway`, `dim_district`, `dim_time_of_day`, `dim_holiday`, `dim_date`) |
| 4 | `sql/02_fact.sql` | `fact_traffic_hour` + `agg_traffic_daily` rollup |
| 5 | `sql/03_seed_dim_date.sql` | Calendar 2018–2030 + sentinel |
| 6 | `sql/03_seed_dim_time_of_day.sql` | 24 rows, peak periods |
| 7 | `sql/03_seed_dim_district.sql` | 12 Caltrans districts |
| 8 | `sql/03_seed_dim_holiday.sql` | Federal + CA holidays 2022–2026 |
| 9 | `sql/03_pipeline_ingest.sql` | `STAGING.merge_pems_staging_deduped(batch_id)` |
| 10 | `sql/03_pipeline_scd2_merge.sql` | `EDW.merge_dim_station_scd2()`, `merge_dim_freeway()`, stubs |
| 11 | `sql/03_pipeline_fact_load.sql` | `EDW.load_fact_traffic_hour(batch_id)`, `refresh_agg_traffic_daily(batch_id)` |
| 12 | `sql/04_views.sql` | Tableau analytics views |

### Phase 2 — Ingest a batch

```sql
USE DATABASE TRAFFIC_PEMS_DB;
USE WAREHOUSE TRAFFIC_PEMS_WH;
SET batch_id = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

COPY INTO TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw (
  ingest_batch_id, file_name, file_row_number,
  station_id, sample_datetime, district, freeway, direction_of_travel,
  lane_type, station_length_mi, samples, pct_observed,
  total_flow_veh, avg_occupancy, avg_speed_mph
)
FROM (
  SELECT
    $batch_id,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    $2::INTEGER,
    TO_TIMESTAMP_NTZ($1, 'MM/DD/YYYY HH24:MI:SS'),
    $3::SMALLINT, $4::SMALLINT, $5::VARCHAR, $6::VARCHAR,
    $7::NUMBER(6,3), $8::INTEGER, $9::NUMBER(5,2),
    $10::NUMBER(10,2), $11::NUMBER(8,6), $12::NUMBER(6,2)
  FROM @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
)
FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS')
ON_ERROR = 'CONTINUE';
```

### Phase 3 — Transform → fact → rollup

```sql
CALL TRAFFIC_PEMS_DB.STAGING.merge_pems_staging_deduped($batch_id);
CALL TRAFFIC_PEMS_DB.EDW.merge_dim_station_scd2();
CALL TRAFFIC_PEMS_DB.EDW.merge_dim_freeway();
CALL TRAFFIC_PEMS_DB.EDW.load_fact_traffic_hour($batch_id);
CALL TRAFFIC_PEMS_DB.EDW.refresh_agg_traffic_daily($batch_id);
```

### Phase 4 — Verify

```sql
SELECT COUNT(*) AS hourly_rows FROM TRAFFIC_PEMS_DB.EDW.fact_traffic_hour;
SELECT COUNT(*) AS daily_rows  FROM TRAFFIC_PEMS_DB.EDW.agg_traffic_daily;
SELECT * FROM TRAFFIC_PEMS_DB.ANALYTICS.v_district_summary LIMIT 20;
```

---

## Execute with Apache Airflow

The DAG `dags/pems_traffic_pipeline_dag.py` runs the same procedures in order, threading a unique `batch_id` from the DAG run.

### 1. Configure the Snowflake connection

Either set `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` in `.env` (see `.env.example`):
```
AIRFLOW_CONN_SNOWFLAKE_DEFAULT=snowflake://USER:PASSWORD@/?account=<id>&warehouse=TRAFFIC_PEMS_WH&database=TRAFFIC_PEMS_DB&role=ACCOUNTADMIN
```
…or use the Airflow UI: **Admin → Connections → snowflake_default**, with extra `{"database": "TRAFFIC_PEMS_DB", "warehouse": "TRAFFIC_PEMS_WH", "role": "ACCOUNTADMIN"}`.

### 2. Optional variables

- `traffic_pems_database` (default `TRAFFIC_PEMS_DB`)
- `traffic_pems_schema_staging` (default `STAGING`)
- `traffic_pems_schema_edw` (default `EDW`)
- `traffic_pems_stage` (default `STG_PEMS_FILES`)

### 3. Run

1. Start Airflow (`docker compose up -d` or `astro dev start`).
2. Open `http://localhost:8080`, find **pems_traffic_pipeline**, unpause, trigger.
3. The DAG runs: `get_batch_id → setup → copy_pems_to_staging → merge_staging_deduped → scd2_dim_station → merge_dim_freeway → load_fact_traffic_hour → refresh_agg_traffic_daily`.

### 4. What the DAG expects

- Files already uploaded to `@STG_PEMS_FILES` (the DAG does not download from PeMS).
- Procedures from `sql/03_pipeline_*.sql` already created in Snowflake (run those once per environment).
- Dimension seeds (`03_seed_dim_*.sql`) already run.

---

## Connect Tableau

1. Tableau Desktop → **Connect → Snowflake**.
2. Server: `<account>.snowflakecomputing.com`. Auth: username/password or SSO.
3. Warehouse: `TRAFFIC_PEMS_WH`. Database: `TRAFFIC_PEMS_DB`. Schema: `ANALYTICS`.
4. Drag in any of:
   - `V_DISTRICT_SUMMARY` — district scorecard (month × district)
   - `V_WAIT_BY_FREEWAY_HOUR` — hour-of-day patterns by freeway
   - `V_HOLIDAY_VS_NORMAL` — holiday vs non-holiday comparison
   - `V_DAY_VS_NIGHT` — daylight vs night by district
   - `V_TOP_BOTTLENECKS` — worst stations with lat/lon for a map
5. For `V_WAIT_BY_FREEWAY_HOUR` and `V_DAY_VS_NIGHT` (which scan `fact_traffic_hour`), use a **Tableau extract refreshed nightly** rather than live to keep dashboards snappy.

---

## Troubleshooting

| Issue | Check |
|-------|-------|
| `COPY INTO` parses zero rows | File format `SKIP_HEADER` matches your file (PeMS hourly = 0, metadata = 1). `LIST @STG_PEMS_FILES;` to confirm files are present. |
| `dim_station` empty after merge | Staging dedupe must run before `merge_dim_station_scd2()`. Confirm `stg_pems_hour_deduped` has rows for the batch. |
| All fact rows have `posted_date_sk = 19000101` | Run `sql/03_seed_dim_date.sql` so the calendar spine exists. |
| Snowflake "not authorized" | Use the same role for every script. Trial accounts: `USE ROLE ACCOUNTADMIN;`. Airflow connection role must match. |
| Tableau dashboards slow | Switch hourly-grain views to an extract, or pre-filter to one district / one month. |
| Statewide load too expensive | Scale `TRAFFIC_PEMS_WH` up to `MEDIUM` for the initial backfill, then `ALTER WAREHOUSE … SET WAREHOUSE_SIZE = 'X-SMALL';` after. |

---

## Procedure quick reference

| Procedure | Schema | Args |
|-----------|--------|------|
| `merge_pems_staging_deduped` | STAGING | `(batch_id VARCHAR)` |
| `merge_dim_station_scd2` | EDW | none |
| `merge_dim_freeway` | EDW | none |
| `merge_dim_district` | EDW | none (no-op; hand-seeded) |
| `merge_dim_holiday` | EDW | none (no-op; hand-seeded) |
| `load_fact_traffic_hour` | EDW | `(batch_id VARCHAR)` |
| `refresh_agg_traffic_daily` | EDW | `(batch_id VARCHAR)` |

All in database `TRAFFIC_PEMS_DB` unless renamed in `01_setup.sql`.
