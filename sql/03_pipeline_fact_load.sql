-- ============================================================
-- 03_pipeline_fact_load.sql — Hourly fact load + daily rollup refresh
-- ============================================================
--
-- load_fact_traffic_hour(batch_id): inserts hourly rows from staging joined
-- to current dim_station + dim_freeway + dim_date + dim_time_of_day.
--   - Free-flow speed default = 65 mph (typical CA mainline posted limit).
--   - delay_min_per_veh = (1/avg_speed - 1/free_flow_speed) × length × 60,
--     clamped to >= 0 (no negative delay when traffic exceeds free-flow).
--   - VMT = flow × length, VHT = flow × length / speed.
--
-- refresh_agg_traffic_daily(batch_id): rebuilds the daily rollup for any
-- date that received new hourly rows in this batch. MERGE-style upsert.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.EDW.load_fact_traffic_hour(batch_id VARCHAR)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    INSERT INTO fact_traffic_hour (
      station_sk, freeway_sk, district_sk, posted_date_sk, hour_sk,
      sample_datetime,
      samples, pct_observed, total_flow_veh, avg_occupancy, avg_speed_mph,
      free_flow_speed_mph, delay_min_per_veh, delay_veh_hours, vmt, vht,
      ingest_batch_id
    )
    SELECT
      st.station_sk,
      fwy.freeway_sk,
      COALESCE(s.district, 0)                          AS district_sk,
      COALESCE(d.date_sk, 19000101)                    AS posted_date_sk,
      EXTRACT(HOUR FROM s.sample_datetime)::SMALLINT   AS hour_sk,
      s.sample_datetime,
      s.samples,
      s.pct_observed,
      s.total_flow_veh,
      s.avg_occupancy,
      s.avg_speed_mph,
      65                                                AS free_flow_speed_mph,
      GREATEST(
        ((1.0 / NULLIF(s.avg_speed_mph, 0)) - (1.0 / 65))
          * COALESCE(s.station_length_mi, 0.5)
          * 60.0,
        0
      )                                                 AS delay_min_per_veh,
      GREATEST(
        ((1.0 / NULLIF(s.avg_speed_mph, 0)) - (1.0 / 65))
          * COALESCE(s.station_length_mi, 0.5)
          * COALESCE(s.total_flow_veh, 0),
        0
      )                                                 AS delay_veh_hours,
      COALESCE(s.total_flow_veh, 0)
        * COALESCE(s.station_length_mi, 0.5)            AS vmt,
      COALESCE(s.total_flow_veh, 0)
        * COALESCE(s.station_length_mi, 0.5)
        / NULLIF(s.avg_speed_mph, 0)                    AS vht,
      :batch_id
    FROM TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_deduped s
    JOIN dim_station st
      ON st.station_nk = s.station_id AND st.is_current
    JOIN dim_freeway fwy
      ON fwy.freeway_number = s.freeway
     AND fwy.direction_of_travel = s.direction_of_travel
    LEFT JOIN dim_date d
      ON d.full_date = DATE(s.sample_datetime)
    WHERE s.ingest_batch_id = :batch_id
      AND NOT EXISTS (
        SELECT 1 FROM fact_traffic_hour f
        WHERE f.station_sk = st.station_sk
          AND f.sample_datetime = s.sample_datetime
      );
    RETURN 'OK: fact_traffic_hour loaded for batch ' || :batch_id;
  END;
  $$;

-- Daily rollup refresh: MERGE for dates that have new hourly rows in this batch
CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.EDW.refresh_agg_traffic_daily(batch_id VARCHAR)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    MERGE INTO agg_traffic_daily t
    USING (
      SELECT
        f.station_sk,
        f.freeway_sk,
        f.district_sk,
        f.posted_date_sk,
        COUNT(*)                                                  AS hours_observed,
        SUM(f.total_flow_veh)                                     AS total_flow_veh,
        AVG(f.avg_occupancy)                                      AS avg_occupancy,
        AVG(f.avg_speed_mph)                                      AS avg_speed_mph,
        MIN(f.avg_speed_mph)                                      AS peak_hour_speed_mph,
        SUM(f.delay_veh_hours)                                    AS total_delay_veh_hours,
        SUM(f.vmt)                                                AS total_vmt,
        SUM(f.vht)                                                AS total_vht
      FROM fact_traffic_hour f
      WHERE f.posted_date_sk IN (
        SELECT DISTINCT posted_date_sk FROM fact_traffic_hour
        WHERE ingest_batch_id = :batch_id
      )
      GROUP BY 1, 2, 3, 4
    ) s
    ON t.station_sk = s.station_sk AND t.posted_date_sk = s.posted_date_sk
    WHEN MATCHED THEN UPDATE SET
      freeway_sk = s.freeway_sk,
      district_sk = s.district_sk,
      hours_observed = s.hours_observed,
      total_flow_veh = s.total_flow_veh,
      avg_occupancy = s.avg_occupancy,
      avg_speed_mph = s.avg_speed_mph,
      peak_hour_speed_mph = s.peak_hour_speed_mph,
      total_delay_veh_hours = s.total_delay_veh_hours,
      total_vmt = s.total_vmt,
      total_vht = s.total_vht,
      ingest_batch_id = :batch_id,
      loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      station_sk, freeway_sk, district_sk, posted_date_sk,
      hours_observed, total_flow_veh, avg_occupancy, avg_speed_mph,
      peak_hour_speed_mph, total_delay_veh_hours, total_vmt, total_vht,
      ingest_batch_id
    ) VALUES (
      s.station_sk, s.freeway_sk, s.district_sk, s.posted_date_sk,
      s.hours_observed, s.total_flow_veh, s.avg_occupancy, s.avg_speed_mph,
      s.peak_hour_speed_mph, s.total_delay_veh_hours, s.total_vmt, s.total_vht,
      :batch_id
    );
    RETURN 'OK: agg_traffic_daily refreshed for batch ' || :batch_id;
  END;
  $$;
