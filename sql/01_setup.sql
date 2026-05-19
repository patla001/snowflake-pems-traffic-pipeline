-- ============================================================
-- 01_setup.sql — Snowflake setup for Caltrans PeMS Traffic Capstone
-- Warehouse, database, schemas, file format, and stage for PeMS CSV files
-- ============================================================
--
-- IMPORTANT: Run the ENTIRE script (select all → Run All).
-- Running only the warehouse block leaves TRAFFIC_PEMS_DB missing.
-- If CREATE DATABASE fails with "not authorized", use a role that can create
-- databases — trial accounts: USE ROLE ACCOUNTADMIN;
--
-- Role tip: Objects created as ACCOUNTADMIN are NOT automatically visible to
-- SYSADMIN. The GRANT block at the end shares this DB/warehouse with SYSADMIN
-- so the Airflow Snowflake connection (or your worksheet default role) can
-- still see it. If you set the Airflow conn role to ACCOUNTADMIN, the grants
-- are redundant but harmless.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- 1) Database and schemas
CREATE DATABASE IF NOT EXISTS TRAFFIC_PEMS_DB
  COMMENT = 'Caltrans PeMS traffic delay/wait-time analytics — dimensional model + SCD2';

CREATE SCHEMA IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING
  COMMENT = 'Landing zone for PeMS hourly station data and station inventory CSVs';

CREATE SCHEMA IF NOT EXISTS TRAFFIC_PEMS_DB.EDW
  COMMENT = 'Enterprise data warehouse — star schema with SCD Type 2 dim_station';

CREATE SCHEMA IF NOT EXISTS TRAFFIC_PEMS_DB.ANALYTICS
  COMMENT = 'Curated views for Tableau and ad-hoc analytics';

-- 2) Warehouse — auto-suspend keeps costs low between loads
-- Note: hourly statewide × 3 years is ~1B rows. Consider scaling to SMALL or
-- MEDIUM temporarily during the initial historical load, then back to X-SMALL.
CREATE WAREHOUSE IF NOT EXISTS TRAFFIC_PEMS_WH
  WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Warehouse for PeMS pipeline; scale up for historical backfills';

-- 3) File format and stage — PeMS Data Clearinghouse exports are gzipped CSV
-- with no header (the docs describe positional columns). Adjust SKIP_HEADER if
-- your downloads include the column row.
CREATE FILE FORMAT IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 0
  NULL_IF = ('', 'NULL', 'null')
  COMPRESSION = 'AUTO'
  EMPTY_FIELD_AS_NULL = TRUE
  COMMENT = 'CSV format for PeMS Data Clearinghouse exports (gzipped, no header)';

CREATE OR REPLACE FILE FORMAT TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS_META
  TYPE = 'CSV'
  FIELD_DELIMITER = '\t'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  COMPRESSION = 'AUTO'
  EMPTY_FIELD_AS_NULL = TRUE
  COMMENT = 'TAB-delimited format for PeMS Station Metadata exports (header row present, 18 columns)';

-- DIRECTORY = ( ENABLE = TRUE ) powers the Snowsight Stages browser.
-- Without it, files uploaded via PUT are still readable by COPY INTO + LIST,
-- but they don't appear in the Snowsight Catalog UI (silent UX trap).
CREATE STAGE IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
  FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS')
  DIRECTORY = ( ENABLE = TRUE )
  COMMENT = 'Stage for PeMS hourly station data files (one folder per district/year recommended)';

CREATE STAGE IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES
  FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS_META')
  DIRECTORY = ( ENABLE = TRUE )
  COMMENT = 'Stage for PeMS station inventory / metadata files';

-- For stages that already existed before the DIRECTORY clause above was added
-- (CREATE STAGE IF NOT EXISTS is a no-op when the stage exists, so won't apply
-- the new option), enable + refresh in place. Both statements are idempotent.
ALTER STAGE TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES        SET DIRECTORY = ( ENABLE = TRUE );
ALTER STAGE TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES   SET DIRECTORY = ( ENABLE = TRUE );
ALTER STAGE TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES        REFRESH;
ALTER STAGE TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES   REFRESH;

-- 4) Share access with SYSADMIN (worksheets often default to SYSADMIN)
GRANT USAGE ON DATABASE TRAFFIC_PEMS_DB TO ROLE SYSADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE TRAFFIC_PEMS_DB TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE TRAFFIC_PEMS_WH TO ROLE SYSADMIN;

USE DATABASE TRAFFIC_PEMS_DB;

SHOW DATABASES LIKE 'TRAFFIC_PEMS_DB';
