# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

SDSU capstone — a **Caltrans PeMS traffic delay analytics** pipeline on Snowflake + Airflow 3 + Tableau. Real freeway loop-detector data, dimensional model with SCD2, hourly fact table plus daily rollup.

## Common commands

### Local Airflow (docker-compose)
```bash
cp -n .env.example .env
docker compose up --build -d         # UI at http://localhost:8080 (admin / admin)
docker compose down -v               # stop + wipe Postgres volume
```
Uses `Dockerfile.local` (image: `apache/airflow:3.2.0`) and `requirements-airflow-docker.txt`. Mounts `./dags`, `./airflow/logs`, `./airflow/plugins`, `./config`.

### Astronomer / Astro Cloud
The **repo root** is the Astro project (`.astro/config.yaml`). Deploys use the root `Dockerfile` (Astro Runtime 3.2) and `requirements.txt`. From the repo root:
```bash
astro login
astro dev start                      # local Astro
astro deploy                         # or: astro deploy <deployment-id>
```

### Tests
```bash
AIRFLOW_HOME=$PWD .venv/bin/python -m pytest tests/ -v
```
Three tests: DAG import errors, tag presence, `default_args["retries"] >= 2`. **Keep `retries >= 2` in any DAG** or the test fails.

### Local Python venv (optional, no Docker)
```bash
pip install -r requirements-airflow.txt
```
**Venv landmine to remember:** Airflow 3 ships as `apache-airflow-core` + `apache-airflow-task-sdk`. If `apache-airflow 2.x` is also installed, you get `BaseXCom` class-identity errors. Keep the venv on Airflow 3 only. Also, `DagBag` lives at `airflow.dag_processing.dagbag` in 3.x, not `airflow.models`.

## Architecture

### Snowflake objects
- Database `TRAFFIC_PEMS_DB`, warehouse `TRAFFIC_PEMS_WH` (X-SMALL, auto-suspend 300s)
- Schemas: `STAGING` (landing + dedupe), `EDW` (star schema), `ANALYTICS` (Tableau views)
- Stages: `STG_PEMS_FILES` (hourly data, no header), `STG_PEMS_META_FILES` (station meta, header row)
- File formats: `FF_CSV_PEMS` (skip_header=0), `FF_CSV_PEMS_META` (skip_header=1)

### Star schema
- **Fact `fact_traffic_hour`** — grain: one row per (station, hour). Clustered by `(posted_date_sk, district_sk)`. ~1B rows statewide × hourly × 3 years.
- **Rollup `agg_traffic_daily`** — pre-aggregated daily fact (24× smaller). **Tableau should hit this by default**; only fall back to hourly when time-of-day is needed.
- **`dim_station`** — SCD Type 2 (lane_count, station_type, freeway, lat/lon can change). Natural key = PeMS `station_id`.
- **`dim_freeway`** — Type 1, derived from current station meta. Compound NK = `(freeway_number, direction_of_travel)`.
- **`dim_district`** — 12 Caltrans districts, hand-seeded.
- **`dim_holiday`** — Federal + CA state holidays 2022–2026, hand-seeded with `is_travel_heavy` flag.
- **`dim_time_of_day`** — 24 rows, peak periods (AM_PEAK / MIDDAY / PM_PEAK / EVENING / OVERNIGHT), `is_daylight_approx`.
- **`dim_date`** — calendar 2018–2030 + `19000101` sentinel for unmatched dates.

### Pipeline execution order (`sql/`)
```
01_setup.sql
02_staging.sql, 02_dimensions_scd2.sql, 02_fact.sql
03_seed_dim_date.sql, _time_of_day.sql, _district.sql, _holiday.sql
03_pipeline_ingest.sql, _scd2_merge.sql, _fact_load.sql
04_views.sql
```
All scripts use fully qualified names and are idempotent (CREATE … IF NOT EXISTS, MERGE for seeds).

### Stored procedures (called by DAG)
| Procedure | Schema | Args |
|-----------|--------|------|
| `merge_pems_staging_deduped` | STAGING | `(batch_id VARCHAR)` |
| `merge_dim_station_scd2` | EDW | none |
| `merge_dim_freeway` | EDW | none |
| `merge_dim_district` | EDW | none (no-op; hand-seeded) |
| `merge_dim_holiday` | EDW | none (no-op; hand-seeded) |
| `load_fact_traffic_hour` | EDW | `(batch_id VARCHAR)` |
| `refresh_agg_traffic_daily` | EDW | `(batch_id VARCHAR)` |

### Derived "wait" measure
```
delay_min_per_veh = MAX( (1/avg_speed − 1/65) × length_mi × 60, 0 )
delay_veh_hours    = MAX( (1/avg_speed − 1/65) × length_mi × total_flow, 0 )
```
65 mph is the free-flow reference (typical CA mainline posted limit). When tuning per lane type (HV / OR / FR), parameterize the constant in `load_fact_traffic_hour`.

### Airflow DAG
- Canonical path: **`dags/pems_traffic_pipeline_dag.py`** (repo root, used by both Astro and docker-compose).
- Authoring API: Airflow 3.x — imports from `airflow.sdk`, schedule uses `schedule=`.
- Connection: `snowflake_default` (UI or `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` URI in `.env`).
- Variables (with defaults): `traffic_pems_database`, `traffic_pems_schema_staging`, `traffic_pems_schema_edw`, `traffic_pems_stage`.
- Task graph: `get_batch_id → setup_snowflake → copy_pems_to_staging → merge_staging_deduped → scd2_dim_station → merge_dim_freeway → load_fact_traffic_hour → refresh_agg_traffic_daily`.
- The DAG **expects files already uploaded** to `@STG_PEMS_FILES` and **procedures already created** (run `sql/03_pipeline_*.sql` once per environment). It does not download from PeMS.

### Tableau integration
- Connect to `TRAFFIC_PEMS_DB.ANALYTICS`.
- Five views: `v_district_summary`, `v_wait_by_freeway_hour`, `v_holiday_vs_normal`, `v_day_vs_night`, `v_top_bottlenecks`.
- Hourly-grain views (`v_wait_by_freeway_hour`, `v_day_vs_night`) scan `fact_traffic_hour` — use a **Tableau extract refreshed nightly** rather than live.

## Conventions worth knowing

- **Two Dockerfiles, two requirements files** — Astro deploy → `Dockerfile` + `requirements.txt`; local compose → `Dockerfile.local` + `requirements-airflow-docker.txt`; local venv → `requirements-airflow.txt`.
- **PeMS files** are gzipped CSVs with **no header** (Station Hour) — file format `SKIP_HEADER=0`. The Meta files **do** have a header.
- **Role / grants:** `01_setup.sql` creates objects as `ACCOUNTADMIN` and grants on the database + warehouse to `SYSADMIN`. If a downstream script complains "not authorized", confirm everything runs as the same role, or extend grants.
- **Batch ID:** the Airflow DAG generates `batch_id` from `logical_date + run_id` and threads it via XCom into every procedure call. Manual runs should use `SET batch_id = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');`.
- **Statewide × hourly × 3 years ≈ 1B rows.** Scale the warehouse up to SMALL/MEDIUM during the initial backfill, then back to X-SMALL. Clustering on `(posted_date_sk, district_sk)` is essential — don't remove it.

## What's NOT in the repo

- A PeMS file downloader script (Caltrans requires manual login; out of scope).
- ML / forecasting code (capstone scope ends at descriptive analytics).
- Real-time / streaming ingest (PeMS publishes batches, not streams).
