-- ============================================================
-- 04_views.sql — Curated analytics views for Tableau
-- Default to agg_traffic_daily; hourly fact only where time-of-day is needed.
-- ============================================================
--
-- Connect Tableau to TRAFFIC_PEMS_DB.ANALYTICS and surface these views.
-- Live connection is fine for daily-grain views; for v_wait_by_freeway_hour
-- (which scans fact_traffic_hour ≈ 1B rows) prefer a Tableau extract
-- refreshed nightly after the DAG completes.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA ANALYTICS;

-- 1) Wait/delay by freeway × hour-of-day across the full available history.
-- Filter dates at the Tableau workbook level (a parameter-driven date range
-- is more useful than a hardcoded cutoff once a multi-year backfill is loaded).
-- Hourly grain ≈ 100M+ rows — use a Tableau extract refreshed nightly, not live.
CREATE OR REPLACE VIEW TRAFFIC_PEMS_DB.ANALYTICS.v_wait_by_freeway_hour AS
SELECT
  f.posted_date_sk,
  d.full_date,
  d.year,
  d.month,
  d.day_name,
  d.is_weekend,
  fwy.freeway_label,
  fwy.freeway_number,
  fwy.direction_of_travel,
  dist.district_sk,
  dist.district_name,
  dist.region,
  tod.hour_sk,
  tod.hour_label,
  tod.peak_period,
  tod.is_peak,
  tod.is_daylight_approx,
  AVG(f.avg_speed_mph)        AS avg_speed_mph,
  AVG(f.delay_min_per_veh)    AS avg_delay_min_per_veh,
  SUM(f.delay_veh_hours)      AS total_delay_veh_hours,
  SUM(f.total_flow_veh)       AS total_flow_veh
FROM TRAFFIC_PEMS_DB.EDW.fact_traffic_hour f
JOIN TRAFFIC_PEMS_DB.EDW.dim_freeway  fwy  ON fwy.freeway_sk  = f.freeway_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_district dist ON dist.district_sk = f.district_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_date     d    ON d.date_sk        = f.posted_date_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_time_of_day tod ON tod.hour_sk    = f.hour_sk
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;

-- 2) Holiday vs non-holiday: same freeway, same calendar window, holiday flag.
-- Uses the daily rollup for speed; surfaces 95th-percentile delay so Tableau
-- can highlight outlier days.
CREATE OR REPLACE VIEW TRAFFIC_PEMS_DB.ANALYTICS.v_holiday_vs_normal AS
SELECT
  d.full_date,
  d.year,
  d.month_name,
  d.day_name,
  fwy.freeway_label,
  dist.district_name,
  dist.region,
  CASE WHEN h.holiday_date IS NOT NULL THEN 'HOLIDAY' ELSE 'NON_HOLIDAY' END AS day_class,
  COALESCE(h.holiday_name, '')      AS holiday_name,
  COALESCE(h.is_travel_heavy, FALSE) AS is_travel_heavy,
  AVG(agg.avg_speed_mph)             AS avg_speed_mph,
  AVG(agg.peak_hour_speed_mph)       AS avg_peak_hour_speed_mph,
  SUM(agg.total_delay_veh_hours)     AS total_delay_veh_hours,
  SUM(agg.total_flow_veh)            AS total_flow_veh
FROM TRAFFIC_PEMS_DB.EDW.agg_traffic_daily agg
JOIN TRAFFIC_PEMS_DB.EDW.dim_freeway  fwy  ON fwy.freeway_sk  = agg.freeway_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_district dist ON dist.district_sk = agg.district_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_date     d    ON d.date_sk        = agg.posted_date_sk
LEFT JOIN TRAFFIC_PEMS_DB.EDW.dim_holiday h ON h.holiday_date  = d.full_date
GROUP BY 1,2,3,4,5,6,7,8,9,10;

-- 3) Daylight vs nighttime patterns by district, full available history.
-- Filter dates in Tableau if you want a recent window. Hourly grain —
-- use an extract for performance, not a live connection.
CREATE OR REPLACE VIEW TRAFFIC_PEMS_DB.ANALYTICS.v_day_vs_night AS
SELECT
  d.full_date,
  d.year,
  d.month,
  d.day_name,
  d.is_weekend,
  dist.district_sk,
  dist.district_name,
  dist.region,
  CASE WHEN tod.is_daylight_approx THEN 'DAYLIGHT' ELSE 'NIGHT' END AS time_class,
  AVG(f.avg_speed_mph)         AS avg_speed_mph,
  AVG(f.delay_min_per_veh)     AS avg_delay_min_per_veh,
  SUM(f.delay_veh_hours)       AS total_delay_veh_hours,
  SUM(f.total_flow_veh)        AS total_flow_veh,
  COUNT(*)                     AS hourly_rows
FROM TRAFFIC_PEMS_DB.EDW.fact_traffic_hour f
JOIN TRAFFIC_PEMS_DB.EDW.dim_district dist ON dist.district_sk = f.district_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_date     d    ON d.date_sk        = f.posted_date_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_time_of_day tod ON tod.hour_sk    = f.hour_sk
GROUP BY 1,2,3,4,5,6,7,8,9;

-- 4) Top bottlenecks: stations ranked by cumulative delay across the full
-- available history. Filter dates in Tableau to scope to a specific year or
-- date range. Plots lat/lon so Tableau can render a map + filter to top N.
CREATE OR REPLACE VIEW TRAFFIC_PEMS_DB.ANALYTICS.v_top_bottlenecks AS
SELECT
  st.station_nk            AS station_id,
  st.station_name,
  st.freeway,
  st.direction_of_travel,
  st.station_type,
  st.lane_count,
  st.latitude,
  st.longitude,
  dist.district_name,
  dist.region,
  d.year,
  COUNT(*)                       AS days_observed,
  AVG(agg.avg_speed_mph)         AS avg_speed_mph,
  AVG(agg.peak_hour_speed_mph)   AS avg_peak_hour_speed_mph,
  AVG(agg.total_delay_veh_hours) AS avg_daily_delay_veh_hours,
  SUM(agg.total_delay_veh_hours) AS sum_delay_veh_hours,
  SUM(agg.total_flow_veh)        AS sum_total_flow_veh
FROM TRAFFIC_PEMS_DB.EDW.agg_traffic_daily agg
JOIN TRAFFIC_PEMS_DB.EDW.dim_station st  ON st.station_sk = agg.station_sk AND st.is_current
JOIN TRAFFIC_PEMS_DB.EDW.dim_district dist ON dist.district_sk = agg.district_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_date     d   ON d.date_sk        = agg.posted_date_sk
WHERE st.station_type = 'ML'   -- mainline only; ramps (FR/OR/FF) lack speed data
  AND agg.total_delay_veh_hours IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
HAVING COUNT(*) >= 10
ORDER BY sum_delay_veh_hours DESC NULLS LAST;

-- 5) District summary: month × district scorecard for executive view.
CREATE OR REPLACE VIEW TRAFFIC_PEMS_DB.ANALYTICS.v_district_summary AS
SELECT
  d.year,
  d.month,
  d.month_name,
  dist.district_sk,
  dist.district_name,
  dist.region,
  COUNT(DISTINCT agg.station_sk)        AS stations_observed,
  COUNT(DISTINCT agg.posted_date_sk)    AS days_observed,
  AVG(agg.avg_speed_mph)                AS avg_speed_mph,
  SUM(agg.total_delay_veh_hours)        AS total_delay_veh_hours,
  SUM(agg.total_flow_veh)               AS total_flow_veh,
  SUM(agg.total_vmt)                    AS total_vmt
FROM TRAFFIC_PEMS_DB.EDW.agg_traffic_daily agg
JOIN TRAFFIC_PEMS_DB.EDW.dim_district dist ON dist.district_sk = agg.district_sk
JOIN TRAFFIC_PEMS_DB.EDW.dim_date     d    ON d.date_sk        = agg.posted_date_sk
GROUP BY 1,2,3,4,5,6;
