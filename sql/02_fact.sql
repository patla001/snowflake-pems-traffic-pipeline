-- ============================================================
-- 02_fact.sql — Fact table + daily rollup for PeMS traffic
-- Grain: one row per (station, hour). Clustered for time-series filtering.
-- ============================================================
--
-- Run after 02_dimensions_scd2.sql (FKs reference dim_station, dim_date, etc).
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

-- Fact: one row per station per hour.
-- Statewide × hourly × 3 years ≈ 1B rows — clustering on (date, district) is
-- essential. Add SEARCH OPTIMIZATION later if station-id lookups dominate.
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.fact_traffic_hour (
  traffic_hour_sk     INTEGER AUTOINCREMENT PRIMARY KEY,
  -- Foreign keys
  station_sk          INTEGER NOT NULL,
  freeway_sk          INTEGER NOT NULL,
  district_sk         INTEGER NOT NULL,
  posted_date_sk      INTEGER NOT NULL,
  hour_sk             SMALLINT NOT NULL,
  -- Degenerate dimensions
  sample_datetime     TIMESTAMP_NTZ NOT NULL,
  -- Measures
  samples             INTEGER,                    -- detector samples used to compute the hour
  pct_observed        NUMBER(5, 2),               -- data quality indicator
  total_flow_veh      NUMBER(10, 2),              -- vehicles passing in the hour
  avg_occupancy       NUMBER(8, 6),               -- 0.0–1.0 fraction of time detector is occupied
  avg_speed_mph       NUMBER(6, 2),
  -- Derived "wait" metrics (computed in load procedure)
  free_flow_speed_mph NUMBER(6, 2),               -- reference, default 65 for ML
  delay_min_per_veh   NUMBER(8, 3),               -- (1/speed - 1/free_flow) * length * 60
  delay_veh_hours     NUMBER(12, 3),              -- delay × flow / 60
  vmt                 NUMBER(14, 3),              -- vehicle-miles traveled in the hour
  vht                 NUMBER(14, 3),              -- vehicle-hours traveled
  -- Pipeline metadata
  ingest_batch_id     VARCHAR(100),
  loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (station_sk, sample_datetime)
);

ALTER TABLE TRAFFIC_PEMS_DB.EDW.fact_traffic_hour
  CLUSTER BY (posted_date_sk, district_sk);

-- Daily rollup: pre-aggregated fact that Tableau hits by default.
-- 24× smaller than fact_traffic_hour; same dims minus hour_sk.
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.agg_traffic_daily (
  station_sk          INTEGER NOT NULL,
  freeway_sk          INTEGER NOT NULL,
  district_sk         INTEGER NOT NULL,
  posted_date_sk      INTEGER NOT NULL,
  hours_observed      SMALLINT,                   -- count of hourly facts feeding this day
  total_flow_veh      NUMBER(14, 2),
  avg_occupancy       NUMBER(8, 6),
  avg_speed_mph       NUMBER(6, 2),
  peak_hour_speed_mph NUMBER(6, 2),               -- min speed across the 24 hours
  total_delay_veh_hours NUMBER(14, 3),
  total_vmt           NUMBER(16, 3),
  total_vht           NUMBER(16, 3),
  ingest_batch_id     VARCHAR(100),
  loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (station_sk, posted_date_sk)
);

ALTER TABLE TRAFFIC_PEMS_DB.EDW.agg_traffic_daily
  CLUSTER BY (posted_date_sk, district_sk);
