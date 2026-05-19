-- ============================================================
-- 03_seed_dim_date.sql — Populate EDW.dim_date (calendar + sentinel)
-- Idempotent MERGE; safe to re-run. Spine covers 2015-01-01 → 2030-12-31.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TRAFFIC_PEMS_WH;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

-- Sentinel row used when sample_datetime has no calendar match
MERGE INTO TRAFFIC_PEMS_DB.EDW.dim_date t
USING (
  SELECT
    19000101 AS date_sk,
    DATE '1900-01-01' AS full_date,
    CAST(1900 AS SMALLINT) AS year,
    CAST(1 AS SMALLINT) AS quarter,
    CAST(1 AS SMALLINT) AS month,
    'Unknown'::VARCHAR(12) AS month_name,
    CAST(1 AS SMALLINT) AS day_of_month,
    CAST(1 AS SMALLINT) AS day_of_week,
    'Unknown'::VARCHAR(12) AS day_name,
    CAST(1 AS SMALLINT) AS week_of_year,
    FALSE AS is_weekend
) s
ON t.date_sk = s.date_sk
WHEN NOT MATCHED THEN
  INSERT (date_sk, full_date, year, quarter, month, month_name,
          day_of_month, day_of_week, day_name, week_of_year, is_weekend)
  VALUES (s.date_sk, s.full_date, s.year, s.quarter, s.month, s.month_name,
          s.day_of_month, s.day_of_week, s.day_name, s.week_of_year, s.is_weekend);

-- Calendar spine: 2015-01-01 → 2030-12-31 (covers full D11 backfill + future loads)
MERGE INTO TRAFFIC_PEMS_DB.EDW.dim_date t
USING (
  SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_sk,
    d AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    MONTHNAME(d) AS month_name,
    EXTRACT(DAY FROM d)::SMALLINT AS day_of_month,
    DAYOFWEEK(d)::SMALLINT AS day_of_week,
    DAYNAME(d) AS day_name,
    WEEKOFYEAR(d)::SMALLINT AS week_of_year,
    (DAYOFWEEK(d) IN (0, 6)) AS is_weekend
  FROM (
    SELECT DATEADD(day, SEQ4(), DATE '2015-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 6000))
  ) spine
  WHERE d <= DATE '2030-12-31'
) s
ON t.date_sk = s.date_sk
WHEN NOT MATCHED THEN
  INSERT (date_sk, full_date, year, quarter, month, month_name,
          day_of_month, day_of_week, day_name, week_of_year, is_weekend)
  VALUES (s.date_sk, s.full_date, s.year, s.quarter, s.month, s.month_name,
          s.day_of_month, s.day_of_week, s.day_name, s.week_of_year, s.is_weekend);
