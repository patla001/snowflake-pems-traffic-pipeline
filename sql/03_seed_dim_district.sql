-- ============================================================
-- 03_seed_dim_district.sql — Caltrans districts 1–12 (Type 1 reference)
-- Idempotent MERGE; safe to re-run.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TRAFFIC_PEMS_WH;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

MERGE INTO TRAFFIC_PEMS_DB.EDW.dim_district t
USING (
  SELECT * FROM VALUES
    (1,  'North Coast',           'NorCal',   'Eureka'),
    (2,  'Northeast',              'NorCal',   'Redding'),
    (3,  'Marysville/Sacramento',  'NorCal',   'Marysville'),
    (4,  'Bay Area',               'NorCal',   'Oakland'),
    (5,  'Central Coast',          'Central',  'San Luis Obispo'),
    (6,  'Central Valley',         'Central',  'Fresno'),
    (7,  'Los Angeles County',     'SoCal',    'Los Angeles'),
    (8,  'Inland Empire',          'SoCal',    'San Bernardino'),
    (9,  'Eastern Sierra',         'Central',  'Bishop'),
    (10, 'San Joaquin Valley',     'Central',  'Stockton'),
    (11, 'San Diego/Imperial',     'SoCal',    'San Diego'),
    (12, 'Orange County',          'SoCal',    'Irvine')
  AS v(district_sk, district_name, region, hq_city)
) s
ON t.district_sk = s.district_sk
WHEN MATCHED THEN UPDATE SET
  district_name = s.district_name,
  region = s.region,
  hq_city = s.hq_city
WHEN NOT MATCHED THEN
  INSERT (district_sk, district_name, region, hq_city)
  VALUES (s.district_sk, s.district_name, s.region, s.hq_city);
