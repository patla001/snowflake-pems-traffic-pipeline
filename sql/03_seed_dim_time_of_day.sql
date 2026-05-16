-- ============================================================
-- 03_seed_dim_time_of_day.sql — 24 rows for hour-of-day reporting
-- Idempotent MERGE; safe to re-run.
-- ============================================================
--
-- Peak windows reflect Caltrans/regional planning conventions:
--   AM_PEAK   06:00 – 08:59
--   MIDDAY    09:00 – 14:59
--   PM_PEAK   15:00 – 18:59
--   EVENING   19:00 – 21:59
--   OVERNIGHT 22:00 – 05:59
-- is_daylight_approx uses a fixed civil window (06–19) — for season-aware
-- daylight, join sample_datetime to a sunrise/sunset table in 04_views.sql.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TRAFFIC_PEMS_WH;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

MERGE INTO TRAFFIC_PEMS_DB.EDW.dim_time_of_day t
USING (
  SELECT
    h::SMALLINT AS hour_sk,
    LPAD(h::VARCHAR, 2, '0') || ':00' AS hour_label,
    CASE
      WHEN h BETWEEN 6 AND 8  THEN 'AM_PEAK'
      WHEN h BETWEEN 9 AND 14 THEN 'MIDDAY'
      WHEN h BETWEEN 15 AND 18 THEN 'PM_PEAK'
      WHEN h BETWEEN 19 AND 21 THEN 'EVENING'
      ELSE 'OVERNIGHT'
    END AS peak_period,
    (h BETWEEN 6 AND 8 OR h BETWEEN 15 AND 18) AS is_peak,
    (h BETWEEN 6 AND 19) AS is_daylight_approx
  FROM (
    SELECT SEQ4() AS h FROM TABLE(GENERATOR(ROWCOUNT => 24))
  )
) s
ON t.hour_sk = s.hour_sk
WHEN MATCHED THEN UPDATE SET
  hour_label = s.hour_label,
  peak_period = s.peak_period,
  is_peak = s.is_peak,
  is_daylight_approx = s.is_daylight_approx
WHEN NOT MATCHED THEN
  INSERT (hour_sk, hour_label, peak_period, is_peak, is_daylight_approx)
  VALUES (s.hour_sk, s.hour_label, s.peak_period, s.is_peak, s.is_daylight_approx);
