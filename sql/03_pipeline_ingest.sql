-- ============================================================
-- 03_pipeline_ingest.sql — PeMS ingest helpers
-- COPY INTO from @STG_PEMS_FILES and dedupe to stg_pems_hour_deduped.
-- ============================================================
--
-- COPY INTO example (run from orchestration with a unique batch_id):
--
--   SET batch_id = (SELECT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'));
--   COPY INTO TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw (
--     ingest_batch_id, file_name, file_row_number,
--     station_id, sample_datetime, district, freeway, direction_of_travel,
--     lane_type, station_length_mi, samples, pct_observed,
--     total_flow_veh, avg_occupancy, avg_speed_mph
--   )
--   FROM (
--     SELECT
--       $batch_id,
--       METADATA$FILENAME,
--       METADATA$FILE_ROW_NUMBER,
--       $2::INTEGER,                                              -- station_id
--       TO_TIMESTAMP_NTZ($1, 'MM/DD/YYYY HH24:MI:SS'),            -- sample_datetime
--       $3::SMALLINT,                                             -- district
--       $4::SMALLINT,                                             -- freeway
--       $5::VARCHAR,                                              -- direction
--       $6::VARCHAR,                                              -- lane_type
--       $7::NUMBER(6,3),                                          -- station_length_mi
--       $8::INTEGER,                                              -- samples
--       $9::NUMBER(5,2),                                          -- pct_observed
--       $10::NUMBER(10,2),                                        -- total_flow_veh
--       $11::NUMBER(8,6),                                         -- avg_occupancy
--       $12::NUMBER(6,2)                                          -- avg_speed_mph
--     FROM @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
--   )
--   FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS')
--   ON_ERROR = 'CONTINUE';
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA STAGING;

-- Dedupe raw → deduped: keep latest row per (station_id, sample_datetime).
-- Tie-break by ingest_ts (most recent wins) so reruns of a file override
-- prior loads cleanly.
CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.STAGING.merge_pems_staging_deduped(batch_id VARCHAR)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    MERGE INTO stg_pems_hour_deduped t
    USING (
      SELECT
        station_id,
        sample_datetime,
        district,
        freeway,
        direction_of_travel,
        lane_type,
        station_length_mi,
        samples,
        pct_observed,
        total_flow_veh,
        avg_occupancy,
        avg_speed_mph,
        ingest_batch_id
      FROM (
        SELECT
          r.*,
          ROW_NUMBER() OVER (
            PARTITION BY station_id, sample_datetime
            ORDER BY ingest_ts DESC
          ) AS rn
        FROM stg_pems_hour_raw r
        WHERE ingest_batch_id = :batch_id
      )
      WHERE rn = 1
    ) s
    ON t.station_id = s.station_id AND t.sample_datetime = s.sample_datetime
    WHEN MATCHED THEN UPDATE SET
      district = s.district,
      freeway = s.freeway,
      direction_of_travel = s.direction_of_travel,
      lane_type = s.lane_type,
      station_length_mi = s.station_length_mi,
      samples = s.samples,
      pct_observed = s.pct_observed,
      total_flow_veh = s.total_flow_veh,
      avg_occupancy = s.avg_occupancy,
      avg_speed_mph = s.avg_speed_mph,
      ingest_batch_id = s.ingest_batch_id
    WHEN NOT MATCHED THEN INSERT (
      station_id, sample_datetime, district, freeway, direction_of_travel,
      lane_type, station_length_mi, samples, pct_observed,
      total_flow_veh, avg_occupancy, avg_speed_mph, ingest_batch_id
    ) VALUES (
      s.station_id, s.sample_datetime, s.district, s.freeway, s.direction_of_travel,
      s.lane_type, s.station_length_mi, s.samples, s.pct_observed,
      s.total_flow_veh, s.avg_occupancy, s.avg_speed_mph, s.ingest_batch_id
    );
    RETURN 'OK: merged PeMS batch ' || :batch_id;
  END;
  $$;
