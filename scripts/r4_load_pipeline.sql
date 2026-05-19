-- Re-runnable Phase 2/3 load: COPY INTO new staged files, then call procs.
-- Snowflake auto-skips files already loaded in the last 64 days.
-- Run with:   snowsql -c pems -f scripts/r4_load_pipeline.sql
USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE WAREHOUSE TRAFFIC_PEMS_WH;

SET batch_id = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
SELECT $batch_id AS batch_id;

-- Phase 2a: hourly files → stg_pems_hour_raw
COPY INTO TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw (
  ingest_batch_id, file_name, file_row_number,
  station_id, sample_datetime, district, freeway, direction_of_travel,
  lane_type, station_length_mi, samples, pct_observed,
  total_flow_veh, avg_occupancy, avg_speed_mph
)
FROM (
  SELECT
    $batch_id, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
    $2::INTEGER, TO_TIMESTAMP_NTZ($1, 'MM/DD/YYYY HH24:MI:SS'),
    $3::SMALLINT, $4::SMALLINT, $5::VARCHAR, $6::VARCHAR,
    $7::NUMBER(6,3), $8::INTEGER, $9::NUMBER(5,2),
    $10::NUMBER(10,2), $11::NUMBER(8,6), $12::NUMBER(6,2)
  FROM @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
)
FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS')
ON_ERROR = 'CONTINUE';

-- Phase 2b: meta files → stg_pems_station_meta_raw (date from filename)
COPY INTO TRAFFIC_PEMS_DB.STAGING.stg_pems_station_meta_raw (
  ingest_batch_id, file_name,
  station_id, meta_effective_date,
  freeway, direction_of_travel, district, county_id, city_id,
  state_pm, abs_pm, latitude, longitude,
  length_mi, station_type, lane_count, station_name
)
FROM (
  SELECT
    $batch_id, METADATA$FILENAME,
    $1::INTEGER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '\\d{4}_\\d{2}_\\d{2}'), 'YYYY_MM_DD'),
    $2::SMALLINT, $3::VARCHAR, $4::SMALLINT, $5::SMALLINT, $6::INTEGER,
    $7::VARCHAR, $8::NUMBER(8,3), $9::NUMBER(9,6), $10::NUMBER(9,6),
    $11::NUMBER(6,3), $12::VARCHAR, $13::SMALLINT, $14::VARCHAR
  FROM @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES
)
FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS_META')
ON_ERROR = 'CONTINUE';

-- Phase 3: dedupe → dims → fact → rollup
CALL TRAFFIC_PEMS_DB.STAGING.merge_pems_staging_deduped($batch_id);
CALL TRAFFIC_PEMS_DB.EDW.merge_dim_station_scd2();
CALL TRAFFIC_PEMS_DB.EDW.merge_dim_freeway();
CALL TRAFFIC_PEMS_DB.EDW.load_fact_traffic_hour($batch_id);
CALL TRAFFIC_PEMS_DB.EDW.refresh_agg_traffic_daily($batch_id);

-- Verify
SELECT year, COUNT(*) AS daily_rows
FROM TRAFFIC_PEMS_DB.EDW.agg_traffic_daily a
JOIN TRAFFIC_PEMS_DB.EDW.dim_date d ON d.date_sk = a.posted_date_sk
GROUP BY year ORDER BY year;
