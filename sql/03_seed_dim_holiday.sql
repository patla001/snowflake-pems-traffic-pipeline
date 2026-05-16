-- ============================================================
-- 03_seed_dim_holiday.sql — Federal + California state holidays 2022–2026
-- Idempotent MERGE; safe to re-run.
-- ============================================================
--
-- Includes traffic-heavy classifications for the holiday-vs-normal analysis.
-- Observed dates: when a federal holiday falls on a weekend, the observed
-- weekday is also inserted (e.g. Christmas 2022 fell on Sunday → Dec 26 row).
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TRAFFIC_PEMS_WH;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

MERGE INTO TRAFFIC_PEMS_DB.EDW.dim_holiday t
USING (
  SELECT * FROM VALUES
    -- 2022
    (DATE '2022-01-01', 'New Year''s Day',                'FEDERAL', TRUE),
    (DATE '2022-01-17', 'Martin Luther King Jr. Day',     'FEDERAL', FALSE),
    (DATE '2022-02-21', 'Presidents'' Day',               'FEDERAL', FALSE),
    (DATE '2022-03-31', 'Cesar Chavez Day',               'STATE',   FALSE),
    (DATE '2022-05-30', 'Memorial Day',                   'FEDERAL', TRUE),
    (DATE '2022-06-19', 'Juneteenth',                     'FEDERAL', FALSE),
    (DATE '2022-06-20', 'Juneteenth (Observed)',          'OBSERVED', FALSE),
    (DATE '2022-07-04', 'Independence Day',               'FEDERAL', TRUE),
    (DATE '2022-09-05', 'Labor Day',                      'FEDERAL', TRUE),
    (DATE '2022-10-10', 'Columbus / Indigenous Peoples Day','FEDERAL', FALSE),
    (DATE '2022-11-11', 'Veterans Day',                   'FEDERAL', FALSE),
    (DATE '2022-11-24', 'Thanksgiving',                   'FEDERAL', TRUE),
    (DATE '2022-11-25', 'Day After Thanksgiving',         'STATE',   TRUE),
    (DATE '2022-12-25', 'Christmas Day',                  'FEDERAL', TRUE),
    (DATE '2022-12-26', 'Christmas Day (Observed)',       'OBSERVED', TRUE),
    -- 2023
    (DATE '2023-01-01', 'New Year''s Day',                'FEDERAL', TRUE),
    (DATE '2023-01-02', 'New Year''s Day (Observed)',     'OBSERVED', TRUE),
    (DATE '2023-01-16', 'Martin Luther King Jr. Day',     'FEDERAL', FALSE),
    (DATE '2023-02-20', 'Presidents'' Day',               'FEDERAL', FALSE),
    (DATE '2023-03-31', 'Cesar Chavez Day',               'STATE',   FALSE),
    (DATE '2023-05-29', 'Memorial Day',                   'FEDERAL', TRUE),
    (DATE '2023-06-19', 'Juneteenth',                     'FEDERAL', FALSE),
    (DATE '2023-07-04', 'Independence Day',               'FEDERAL', TRUE),
    (DATE '2023-09-04', 'Labor Day',                      'FEDERAL', TRUE),
    (DATE '2023-10-09', 'Columbus / Indigenous Peoples Day','FEDERAL', FALSE),
    (DATE '2023-11-10', 'Veterans Day (Observed)',        'OBSERVED', FALSE),
    (DATE '2023-11-11', 'Veterans Day',                   'FEDERAL', FALSE),
    (DATE '2023-11-23', 'Thanksgiving',                   'FEDERAL', TRUE),
    (DATE '2023-11-24', 'Day After Thanksgiving',         'STATE',   TRUE),
    (DATE '2023-12-25', 'Christmas Day',                  'FEDERAL', TRUE),
    -- 2024
    (DATE '2024-01-01', 'New Year''s Day',                'FEDERAL', TRUE),
    (DATE '2024-01-15', 'Martin Luther King Jr. Day',     'FEDERAL', FALSE),
    (DATE '2024-02-19', 'Presidents'' Day',               'FEDERAL', FALSE),
    (DATE '2024-03-31', 'Cesar Chavez Day',               'STATE',   FALSE),
    (DATE '2024-04-01', 'Cesar Chavez Day (Observed)',    'OBSERVED', FALSE),
    (DATE '2024-05-27', 'Memorial Day',                   'FEDERAL', TRUE),
    (DATE '2024-06-19', 'Juneteenth',                     'FEDERAL', FALSE),
    (DATE '2024-07-04', 'Independence Day',               'FEDERAL', TRUE),
    (DATE '2024-09-02', 'Labor Day',                      'FEDERAL', TRUE),
    (DATE '2024-10-14', 'Columbus / Indigenous Peoples Day','FEDERAL', FALSE),
    (DATE '2024-11-11', 'Veterans Day',                   'FEDERAL', FALSE),
    (DATE '2024-11-28', 'Thanksgiving',                   'FEDERAL', TRUE),
    (DATE '2024-11-29', 'Day After Thanksgiving',         'STATE',   TRUE),
    (DATE '2024-12-25', 'Christmas Day',                  'FEDERAL', TRUE),
    -- 2025
    (DATE '2025-01-01', 'New Year''s Day',                'FEDERAL', TRUE),
    (DATE '2025-01-20', 'Martin Luther King Jr. Day',     'FEDERAL', FALSE),
    (DATE '2025-02-17', 'Presidents'' Day',               'FEDERAL', FALSE),
    (DATE '2025-03-31', 'Cesar Chavez Day',               'STATE',   FALSE),
    (DATE '2025-05-26', 'Memorial Day',                   'FEDERAL', TRUE),
    (DATE '2025-06-19', 'Juneteenth',                     'FEDERAL', FALSE),
    (DATE '2025-07-04', 'Independence Day',               'FEDERAL', TRUE),
    (DATE '2025-09-01', 'Labor Day',                      'FEDERAL', TRUE),
    (DATE '2025-10-13', 'Columbus / Indigenous Peoples Day','FEDERAL', FALSE),
    (DATE '2025-11-11', 'Veterans Day',                   'FEDERAL', FALSE),
    (DATE '2025-11-27', 'Thanksgiving',                   'FEDERAL', TRUE),
    (DATE '2025-11-28', 'Day After Thanksgiving',         'STATE',   TRUE),
    (DATE '2025-12-25', 'Christmas Day',                  'FEDERAL', TRUE),
    -- 2026
    (DATE '2026-01-01', 'New Year''s Day',                'FEDERAL', TRUE),
    (DATE '2026-01-19', 'Martin Luther King Jr. Day',     'FEDERAL', FALSE),
    (DATE '2026-02-16', 'Presidents'' Day',               'FEDERAL', FALSE),
    (DATE '2026-03-31', 'Cesar Chavez Day',               'STATE',   FALSE),
    (DATE '2026-05-25', 'Memorial Day',                   'FEDERAL', TRUE),
    (DATE '2026-06-19', 'Juneteenth',                     'FEDERAL', FALSE),
    (DATE '2026-07-04', 'Independence Day',               'FEDERAL', TRUE),
    (DATE '2026-09-07', 'Labor Day',                      'FEDERAL', TRUE),
    (DATE '2026-10-12', 'Columbus / Indigenous Peoples Day','FEDERAL', FALSE),
    (DATE '2026-11-11', 'Veterans Day',                   'FEDERAL', FALSE),
    (DATE '2026-11-26', 'Thanksgiving',                   'FEDERAL', TRUE),
    (DATE '2026-11-27', 'Day After Thanksgiving',         'STATE',   TRUE),
    (DATE '2026-12-25', 'Christmas Day',                  'FEDERAL', TRUE)
  AS v(holiday_date, holiday_name, holiday_type, is_travel_heavy)
) s
ON t.holiday_date = s.holiday_date
WHEN MATCHED THEN UPDATE SET
  holiday_name = s.holiday_name,
  holiday_type = s.holiday_type,
  is_travel_heavy = s.is_travel_heavy
WHEN NOT MATCHED THEN
  INSERT (holiday_date, holiday_name, holiday_type, is_travel_heavy)
  VALUES (s.holiday_date, s.holiday_name, s.holiday_type, s.is_travel_heavy);
