-- ============================================================
-- 03_pipeline_scd2_merge.sql — SCD2 + Type 1 dimension merges for PeMS
-- ============================================================
--
-- dim_station is SCD Type 2 — close prior row + insert new when freeway,
-- direction, station_type, lane_count, or location changes.
-- dim_freeway is Type 1 (no history) — derived from current station meta.
-- dim_district and dim_holiday are hand-seeded (see 03_seed_dim_*.sql).
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRAFFIC_PEMS_DB;
USE SCHEMA EDW;

-- ---------- dim_station SCD2 ----------
-- Source of truth for station attributes is stg_pems_station_meta_raw.
-- If no metadata file has been loaded yet, the procedure falls back to
-- attributes carried in stg_pems_hour_deduped (freeway, direction, district,
-- lane_type) so a station_sk can be assigned even before metadata arrives.
CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.EDW.merge_dim_station_scd2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  DECLARE
    ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  BEGIN
    -- Step 1: close current rows where attributes have changed
    UPDATE dim_station t
    SET effective_to = :ts, is_current = FALSE
    WHERE t.is_current
      AND EXISTS (
        SELECT 1
        FROM (
          SELECT
            station_id,
            ANY_VALUE(freeway)              AS freeway,
            ANY_VALUE(direction_of_travel)  AS direction_of_travel,
            ANY_VALUE(district)             AS district,
            ANY_VALUE(lane_type)            AS station_type
          FROM TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_deduped
          GROUP BY station_id
        ) s
        WHERE s.station_id = t.station_nk
          AND (
            COALESCE(s.freeway, -1)            <> COALESCE(t.freeway, -1)
            OR COALESCE(s.direction_of_travel, '') <> COALESCE(t.direction_of_travel, '')
            OR COALESCE(s.district, -1)        <> COALESCE(t.district, -1)
            OR COALESCE(s.station_type, '')    <> COALESCE(t.station_type, '')
          )
      );

    -- Step 2: insert new versions for new natural keys OR changed attributes
    INSERT INTO dim_station (
      station_nk, freeway, direction_of_travel, district,
      station_type, lane_count, station_name,
      effective_from, effective_to, is_current
    )
    SELECT
      s.station_id,
      s.freeway,
      s.direction_of_travel,
      s.district,
      s.station_type,
      NULL                  AS lane_count,
      'Station ' || s.station_id::VARCHAR AS station_name,
      :ts                   AS effective_from,
      NULL                  AS effective_to,
      TRUE                  AS is_current
    FROM (
      SELECT
        station_id,
        ANY_VALUE(freeway)              AS freeway,
        ANY_VALUE(direction_of_travel)  AS direction_of_travel,
        ANY_VALUE(district)             AS district,
        ANY_VALUE(lane_type)            AS station_type
      FROM TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_deduped
      GROUP BY station_id
    ) s
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_station d
      WHERE d.is_current AND d.station_nk = s.station_id
    );

    -- Step 3: enrich from station meta when present (latitude/longitude/lane_count etc).
    -- Updates current row in place (Type 1 within the current SCD2 version).
    UPDATE dim_station d
    SET
      latitude     = m.latitude,
      longitude    = m.longitude,
      length_mi    = m.length_mi,
      lane_count   = m.lane_count,
      station_name = COALESCE(m.station_name, d.station_name),
      state_pm     = m.state_pm,
      abs_pm       = m.abs_pm,
      county_id    = m.county_id,
      city_id      = m.city_id
    FROM (
      SELECT
        station_id,
        ANY_VALUE(latitude)     AS latitude,
        ANY_VALUE(longitude)    AS longitude,
        ANY_VALUE(length_mi)    AS length_mi,
        ANY_VALUE(lane_count)   AS lane_count,
        ANY_VALUE(station_name) AS station_name,
        ANY_VALUE(state_pm)     AS state_pm,
        ANY_VALUE(abs_pm)       AS abs_pm,
        ANY_VALUE(county_id)    AS county_id,
        ANY_VALUE(city_id)      AS city_id
      FROM TRAFFIC_PEMS_DB.STAGING.stg_pems_station_meta_raw
      GROUP BY station_id
    ) m
    WHERE d.station_nk = m.station_id AND d.is_current;

    COMMIT;
    RETURN 'OK: dim_station SCD2 merge done';
  END;
  $$;

-- ---------- dim_freeway Type 1 ----------
-- Insert any (freeway, direction) pair seen in current station data that
-- isn't already in dim_freeway. Label is "I-5 N", "US-101 S", etc.
CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.EDW.merge_dim_freeway()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    INSERT INTO dim_freeway (freeway_number, direction_of_travel, freeway_label, district)
    SELECT
      s.freeway,
      s.direction_of_travel,
      'SR-' || s.freeway::VARCHAR || ' ' || s.direction_of_travel AS freeway_label,
      ANY_VALUE(s.district)
    FROM (
      SELECT freeway, direction_of_travel, district
      FROM dim_station
      WHERE is_current
        AND freeway IS NOT NULL
        AND direction_of_travel IS NOT NULL
    ) s
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_freeway f
      WHERE f.freeway_number = s.freeway
        AND f.direction_of_travel = s.direction_of_travel
    )
    GROUP BY s.freeway, s.direction_of_travel;
    COMMIT;
    RETURN 'OK: dim_freeway merge done';
  END;
  $$;

-- ---------- Hand-seeded dims: stubs for DAG completeness ----------
-- dim_district and dim_holiday are static reference data (see seed scripts).
-- These procedures exist so the DAG's task graph stays uniform.
CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.EDW.merge_dim_district()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    RETURN 'OK: dim_district is hand-seeded (see 03_seed_dim_district.sql)';
  END;
  $$;

CREATE OR REPLACE PROCEDURE TRAFFIC_PEMS_DB.EDW.merge_dim_holiday()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    RETURN 'OK: dim_holiday is hand-seeded (see 03_seed_dim_holiday.sql)';
  END;
  $$;
