"""
PeMS Traffic Pipeline DAG — Caltrans Snowflake ingest, SCD2 station merge, hourly fact load, daily rollup.
Apache Airflow 3.x: authoring imports from airflow.sdk; schedule uses `schedule` (not schedule_interval).
Connection: snowflake_default — Admin → Connections → Snowflake (see docs/PIPELINE_EXECUTION.md).
"""

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE = Variable.get("traffic_pems_database", default="TRAFFIC_PEMS_DB")
SCHEMA_STAGING = Variable.get("traffic_pems_schema_staging", default="STAGING")
SCHEMA_EDW = Variable.get("traffic_pems_schema_edw", default="EDW")
STAGE = Variable.get("traffic_pems_stage", default="STG_PEMS_FILES")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="pems_traffic_pipeline",
    default_args=default_args,
    description="Caltrans PeMS hourly traffic: COPY → staging merge → SCD2 station → hourly fact → daily rollup",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "pems", "traffic", "scd2"],
)


def _push_batch_id(**context):
    run_id = (context.get("run_id") or "manual")[:50]
    logical_date = context.get("logical_date")
    if logical_date:
        bid = logical_date.strftime("%Y%m%d_%H%M%S") + "_" + run_id.replace(":", "_").replace("+", "_")[-12:]
    else:
        bid = run_id.replace(":", "_").replace("+", "_") or "manual"
    context["ti"].xcom_push(key="batch_id", value=bid)
    return bid


task_get_batch_id = PythonOperator(
    task_id="get_batch_id",
    dag=dag,
    python_callable=_push_batch_id,
)

# Idempotent setup — safe to run every DAG execution
task_setup = SQLExecuteQueryOperator(
    task_id="setup_snowflake",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    CREATE WAREHOUSE IF NOT EXISTS TRAFFIC_PEMS_WH
      WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE
      INITIALLY_SUSPENDED = TRUE COMMENT = 'PeMS traffic capstone';
    CREATE DATABASE IF NOT EXISTS TRAFFIC_PEMS_DB COMMENT = 'PeMS traffic capstone';
    CREATE SCHEMA IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING COMMENT = 'Staging';
    CREATE SCHEMA IF NOT EXISTS TRAFFIC_PEMS_DB.EDW COMMENT = 'EDW';
    CREATE SCHEMA IF NOT EXISTS TRAFFIC_PEMS_DB.ANALYTICS COMMENT = 'Analytics';
    CREATE FILE FORMAT IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS
      TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 0
      NULL_IF = ('', 'NULL', 'null') COMPRESSION = 'AUTO' EMPTY_FIELD_AS_NULL = TRUE;
    CREATE STAGE IF NOT EXISTS TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
      FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS');
    """,
)

# COPY INTO from stage. Files in @STG_PEMS_FILES must be PeMS Station Hour
# exports (gzipped CSV, no header, positional columns).
# If you process the stage in batches (e.g. one district/month folder per run),
# replace @STG_PEMS_FILES with the path the orchestrator supplies as a param.
task_copy_to_staging = SQLExecuteQueryOperator(
    task_id="copy_pems_to_staging",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    COPY INTO TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw (
      ingest_batch_id, file_name, file_row_number,
      station_id, sample_datetime, district, freeway, direction_of_travel,
      lane_type, station_length_mi, samples, pct_observed,
      total_flow_veh, avg_occupancy, avg_speed_mph
    )
    FROM (
      SELECT
        '{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        $2::INTEGER,
        TO_TIMESTAMP_NTZ($1, 'MM/DD/YYYY HH24:MI:SS'),
        $3::SMALLINT,
        $4::SMALLINT,
        $5::VARCHAR,
        $6::VARCHAR,
        $7::NUMBER(6,3),
        $8::INTEGER,
        $9::NUMBER(5,2),
        $10::NUMBER(10,2),
        $11::NUMBER(8,6),
        $12::NUMBER(6,2)
      FROM @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
    )
    FILE_FORMAT = (FORMAT_NAME = 'TRAFFIC_PEMS_DB.STAGING.FF_CSV_PEMS')
    ON_ERROR = 'CONTINUE';
    """,
)

task_merge_staging = SQLExecuteQueryOperator(
    task_id="merge_staging_deduped",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL TRAFFIC_PEMS_DB.STAGING.merge_pems_staging_deduped("
        "'{{ ti.xcom_pull(task_ids=\"get_batch_id\", key=\"batch_id\") }}');",
)

# SCD2 + Type 1 dimension merges (run in parallel after staging)
task_scd2_station = SQLExecuteQueryOperator(
    task_id="scd2_dim_station",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL TRAFFIC_PEMS_DB.EDW.merge_dim_station_scd2();",
)
task_merge_freeway = SQLExecuteQueryOperator(
    task_id="merge_dim_freeway",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL TRAFFIC_PEMS_DB.EDW.merge_dim_freeway();",
)

task_load_fact = SQLExecuteQueryOperator(
    task_id="load_fact_traffic_hour",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL TRAFFIC_PEMS_DB.EDW.load_fact_traffic_hour("
        "'{{ ti.xcom_pull(task_ids=\"get_batch_id\", key=\"batch_id\") }}');",
)

task_refresh_rollup = SQLExecuteQueryOperator(
    task_id="refresh_agg_traffic_daily",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL TRAFFIC_PEMS_DB.EDW.refresh_agg_traffic_daily("
        "'{{ ti.xcom_pull(task_ids=\"get_batch_id\", key=\"batch_id\") }}');",
)

# Graph: batch_id → setup → COPY → staging merge → [SCD2 station, freeway] → fact → rollup
task_get_batch_id >> task_setup >> task_copy_to_staging >> task_merge_staging
task_merge_staging >> task_scd2_station >> task_merge_freeway >> task_load_fact >> task_refresh_rollup
