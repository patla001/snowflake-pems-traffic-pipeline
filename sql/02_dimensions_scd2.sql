-- ============================================================
-- 02_dimensions_scd2.sql — Star-schema dimensions for PeMS traffic
-- dim_station is SCD Type 2 (configuration changes over time); others are
-- mostly Type 1 reference dims plus a conformed dim_date / dim_time_of_day.
-- ============================================================
--
-- Prerequisite: 01_setup.sql (TRAFFIC_PEMS_DB.EDW exists).
-- Three-part names everywhere so DDL is independent of session context.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TRAFFIC_PEMS_WH;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

-- Dim Station: SCD Type 2.
-- Caltrans does reconfigure stations (lane count, type, mile-marker corrections),
-- so a single station_id may have multiple effective-dated versions.
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.dim_station (
  station_sk          INTEGER AUTOINCREMENT PRIMARY KEY,
  station_nk          INTEGER NOT NULL,           -- PeMS station_id
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
  station_type        VARCHAR(4),                 -- ML, HV, OR, FR, FF, CD, CH, HOV
  lane_count          SMALLINT,
  station_name        VARCHAR(200),
  -- SCD2 columns
  effective_from      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  effective_to        TIMESTAMP_NTZ,
  is_current          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dim Freeway: Type 1 reference dim, derived from station meta
-- Compound natural key (freeway_number + direction) — N-101 and S-101 are
-- different reporting units operationally.
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.dim_freeway (
  freeway_sk          INTEGER AUTOINCREMENT PRIMARY KEY,
  freeway_number      SMALLINT NOT NULL,
  direction_of_travel VARCHAR(2) NOT NULL,
  freeway_label       VARCHAR(50),                -- e.g. "I-5 N", "US-101 S"
  district            SMALLINT,
  created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (freeway_number, direction_of_travel)
);

-- Dim District: Caltrans Districts 1–12 (Type 1, hand-curated labels)
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.dim_district (
  district_sk         INTEGER PRIMARY KEY,        -- == district number
  district_name       VARCHAR(100) NOT NULL,
  region              VARCHAR(50),                -- NorCal / Central / SoCal
  hq_city             VARCHAR(100)
);

-- Dim Time of Day: 24 rows, conformed across reports
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.dim_time_of_day (
  hour_sk             SMALLINT PRIMARY KEY,       -- 0–23
  hour_label          VARCHAR(8),                 -- "00:00", "13:00"
  peak_period         VARCHAR(16),                -- AM_PEAK / MIDDAY / PM_PEAK / EVENING / OVERNIGHT
  is_peak             BOOLEAN,
  is_daylight_approx  BOOLEAN                     -- 06:00–19:59 default; refined per season in views if needed
);

-- Dim Holiday: federal + CA state holidays (Type 1, seeded for 2022–2026)
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.dim_holiday (
  holiday_date        DATE PRIMARY KEY,
  holiday_name        VARCHAR(100) NOT NULL,
  holiday_type        VARCHAR(20) NOT NULL,       -- FEDERAL / STATE / OBSERVED
  is_travel_heavy     BOOLEAN                     -- Thanksgiving, July 4, Memorial, Labor
);

-- Dim Date: conformed reporting dimension. Populated by 03_seed_dim_date.sql.
CREATE TABLE IF NOT EXISTS TRAFFIC_PEMS_DB.EDW.dim_date (
  date_sk             INTEGER NOT NULL PRIMARY KEY,
  full_date           DATE NOT NULL,
  year                SMALLINT,
  quarter             SMALLINT,
  month               SMALLINT,
  month_name          VARCHAR(12),
  day_of_month        SMALLINT,
  day_of_week         SMALLINT,                   -- 0=Sun … 6=Sat
  day_name            VARCHAR(12),
  week_of_year        SMALLINT,
  is_weekend          BOOLEAN
);
