-- ============================================================
-- 02_staging.sql — Landing layer for PeMS Data Clearinghouse exports
-- Two raw tables: hourly station readings + station metadata inventory
-- ============================================================
--
-- PeMS file references (Data Clearinghouse → Type = "Station Hour" / "Meta"):
--   Hourly Station Data — positional columns, no header in the .gz download
--   Station Metadata    — header row, semi-static inventory of detector locations
--
-- Natural keys:
--   stg_pems_hour_raw    : (station_id, sample_datetime)
--   stg_pems_station_meta_raw : (station_id, meta_effective_date)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA STAGING;

-- Hourly station observations as landed from PeMS Station Hour exports.
-- PeMS column order (5-min and hour exports share the first ~12 columns):
--   1  Timestamp (e.g. 01/01/2024 13:00:00)
--   2  Station ID
--   3  District
--   4  Freeway
--   5  Direction of Travel
--   6  Lane Type
--   7  Station Length (mi)
--   8  Samples (count)
--   9  % Observed
--   10 Total Flow (vehicles in the hour)
--   11 Avg Occupancy (0.0–1.0)
--   12 Avg Speed (mph)
--   (per-lane columns 13+ are intentionally not staged here)
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw (
  ingest_batch_id     VARCHAR(100) NOT NULL,
  ingest_ts           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  file_name           VARCHAR(500),
  file_row_number     INTEGER,
  -- Natural key
  station_id          INTEGER NOT NULL,
  sample_datetime     TIMESTAMP_NTZ NOT NULL,
  -- PeMS columns (positional)
  district            SMALLINT,
  freeway             SMALLINT,
  direction_of_travel VARCHAR(2),
  lane_type           VARCHAR(4),
  station_length_mi   NUMBER(6, 3),
  samples             INTEGER,
  pct_observed        NUMBER(5, 2),
  total_flow_veh      NUMBER(10, 2),
  avg_occupancy       NUMBER(8, 6),
  avg_speed_mph       NUMBER(6, 2),
  PRIMARY KEY (station_id, sample_datetime, ingest_batch_id)
);

ALTER TABLE TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw
  CLUSTER BY (DATE_TRUNC('DAY', sample_datetime), district);

-- Deduped staging: one row per (station, sample_datetime) per batch
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_deduped (
  station_id          INTEGER NOT NULL,
  sample_datetime     TIMESTAMP_NTZ NOT NULL,
  district            SMALLINT,
  freeway             SMALLINT,
  direction_of_travel VARCHAR(2),
  lane_type           VARCHAR(4),
  station_length_mi   NUMBER(6, 3),
  samples             INTEGER,
  pct_observed        NUMBER(5, 2),
  total_flow_veh      NUMBER(10, 2),
  avg_occupancy       NUMBER(8, 6),
  avg_speed_mph       NUMBER(6, 2),
  ingest_batch_id     VARCHAR(100) NOT NULL,
  PRIMARY KEY (station_id, sample_datetime)
);

ALTER TABLE TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_deduped
  CLUSTER BY (DATE_TRUNC('DAY', sample_datetime), district);

-- Station metadata inventory (Meta export). Effective-dated by Caltrans;
-- changes capture mile-marker corrections, lane reconfigurations, etc.
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.stg_pems_station_meta_raw (
  ingest_batch_id     VARCHAR(100) NOT NULL,
  ingest_ts           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  file_name           VARCHAR(500),
  -- Natural key
  station_id          INTEGER NOT NULL,
  meta_effective_date DATE NOT NULL,
  -- PeMS Meta columns
  freeway             SMALLINT,
  direction_of_travel VARCHAR(2),
  district            SMALLINT,
  county_id           SMALLINT,
  city_id             INTEGER,
  state_pm            VARCHAR(20),
  abs_pm              NUMBER(8, 3),
  latitude            NUMBER(9, 6),
  longitude           NUMBER(9, 6),
  length_mi           NUMBER(6, 3),
  station_type        VARCHAR(4),    -- ML, HV, OR, FR, FF, CD, CH, HOV
  lane_count          SMALLINT,
  station_name        VARCHAR(200),
  PRIMARY KEY (station_id, meta_effective_date)
);
