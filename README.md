# Caltrans PeMS Traffic Analytics — Capstone Project

A capstone analyzing **California freeway traffic delay and "wait time" patterns** using real Caltrans PeMS detector data. Built on **Snowflake** for the data warehouse, **Apache Airflow** for orchestration, and **Tableau** for dashboards.

## Overview

- **Question:** When and where do Californians wait the longest in traffic, and how do holidays / time of day / daylight change the pattern?
- **Data:** [Caltrans Performance Measurement System (PeMS)](https://pems.dot.ca.gov) — 30-second freeway loop-detector readings aggregated to hourly Station data, statewide, ~2022–2024 (3 years × hourly × ~40k stations ≈ 1B rows).
- **Stack:** Snowflake (warehouse + dimensional model with SCD2), Airflow 3.x (ingest + transform DAG), Tableau (dashboards on the `ANALYTICS` schema).
- **Pipeline execution:** see **[docs/PIPELINE_EXECUTION.md](docs/PIPELINE_EXECUTION.md)** for PeMS signup, file download, Snowflake script order, and the Airflow DAG.

## Project structure

```
snowflake/
├── README.md
├── .astro/config.yaml             # Astro project marker (deploy from repo root)
├── Dockerfile                     # Astro Runtime 3.2 (Airflow 3) — for Astronomer / Astro Cloud
├── Dockerfile.local               # apache/airflow:3.2 — for local docker-compose only
├── docker-compose.yaml
├── docs/PIPELINE_EXECUTION.md     # PeMS registration + Snowflake + Airflow + Tableau steps
├── dags/pems_traffic_pipeline_dag.py
├── sql/
│   ├── 01_setup.sql                       # Warehouse, DB, schemas, file formats, stages
│   ├── 02_staging.sql                     # stg_pems_hour_raw / deduped + station meta
│   ├── 02_dimensions_scd2.sql             # dim_station (SCD2), dim_freeway, dim_district, …
│   ├── 02_fact.sql                        # fact_traffic_hour + agg_traffic_daily rollup
│   ├── 03_seed_dim_date.sql               # Calendar 2018–2030 + sentinel
│   ├── 03_seed_dim_time_of_day.sql        # 24 hours, peak periods, daylight flag
│   ├── 03_seed_dim_district.sql           # 12 Caltrans districts
│   ├── 03_seed_dim_holiday.sql            # Federal + CA holidays 2022–2026
│   ├── 03_pipeline_ingest.sql             # merge_pems_staging_deduped procedure
│   ├── 03_pipeline_scd2_merge.sql         # SCD2 + dim_freeway procedures
│   ├── 03_pipeline_fact_load.sql          # Fact load + daily rollup procedures
│   └── 04_views.sql                       # ANALYTICS views for Tableau
├── tests/dags/                    # Airflow DAG import / retries / tag tests
├── packages.txt                   # OS packages for Astro Runtime ONBUILD (may be empty)
├── requirements.txt               # Astro image — provider deps only
├── requirements-airflow.txt       # Local venv — Airflow 3 + providers
└── requirements-airflow-docker.txt # Local docker-compose image
```

## Data model

```
                  ┌──────────────────────────┐
                  │  fact_traffic_hour       │
                  │  grain: station × hour   │◀─┐
                  │  + agg_traffic_daily     │  │
                  └──────────────────────────┘  │
                              │                 │
   ┌────────────────┬─────────┴──────┬──────────┴────────┬───────────────┐
   ▼                ▼                ▼                   ▼               ▼
dim_station    dim_freeway     dim_district        dim_date         dim_time_of_day
(SCD2)         (Type 1)        (1–12, seeded)      (calendar)       (24 rows)

                              dim_holiday (CA + federal 2022–2026)
```

Derived "wait" measure:
```
delay_min_per_veh = max( (1/avg_speed − 1/65) × length × 60, 0 )
delay_veh_hours    = max( (1/avg_speed − 1/65) × length × total_flow, 0 )
```
65 mph is the free-flow reference (typical California mainline posted limit; tune per lane type if needed).

## Quick start

1. **Register for PeMS** (`pems.dot.ca.gov`) — Caltrans approval is manual, takes 1–3 business days.
2. **Download** "Station Hour" exports for the districts/years you want (see [docs/PIPELINE_EXECUTION.md](docs/PIPELINE_EXECUTION.md)).
3. **Run** `sql/01_setup.sql` → `02_*.sql` → `03_seed_*.sql` → `03_pipeline_*.sql` → `04_views.sql` in Snowflake (in that order, idempotent).
4. **Upload** files to `@TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES` via Snowsight or `PUT`.
5. **Trigger** the `pems_traffic_pipeline` Airflow DAG (or run the Phase 2/3 SQL manually).
6. **Connect Tableau** → Snowflake → `TRAFFIC_PEMS_DB.ANALYTICS` schema → build dashboards on `v_*` views.

## Local development

```bash
cp -n .env.example .env
docker compose up --build -d        # Airflow UI at http://localhost:8080 (admin / admin)
pytest tests/                       # DAG import / tags / retries checks
```

## Deploy to Astronomer

The **repo root** is the Astro project (`.astro/config.yaml`). Use the root `Dockerfile` + `requirements.txt`.
```bash
astro login
astro deploy                        # or: astro deploy <deployment-id>
```

## License

For academic use; adapt as needed for course requirements.
