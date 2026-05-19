#!/bin/bash
# r3_backfill_year.sh — Upload one year of D11 PeMS files to Snowflake stages.
# Run `./scripts/r3_backfill_year.sh --help` for full usage.
set -euo pipefail

log()       { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
log_step()  { printf '\n[%s] ==> %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
elapsed()   { local s=$1 e=$2; printf '%dm%ds' $(( (e-s)/60 )) $(( (e-s)%60 )); }

usage() {
  cat <<'EOF'
r3_backfill_year.sh — Upload one year of D11 PeMS files to Snowflake stages.

USAGE
  ./scripts/r3_backfill_year.sh <year>
  ./scripts/r3_backfill_year.sh -h | --help

ARGUMENTS
  <year>   4-digit year to back-fill (e.g. 2023). Files matching
           data/station-hour/d11_text_station_hour_<year>_*.txt.gz and
           data/metadata/d11_text_meta_<year>_*.txt are uploaded.

OPTIONS
  -h, --help   Show this help and exit.

AUTHENTICATION
  The script needs the password for the SnowSQL connection profile named "pems"
  (configured in ~/.snowsql/config). It will prompt for the password once at the
  start, hide your input, and export it to SnowSQL via SNOWSQL_PWD so every
  subsequent snowsql call reuses it. The password is never written to disk and
  is unset from the environment on exit. To skip the prompt (CI / unattended),
  pre-export SNOWSQL_PWD before invoking the script.

WHAT IT DOES
  1. Prompts for the Snowflake password (unless SNOWSQL_PWD is already set).
  2. Scans the local data/ folders for files matching the given year and logs
     the file count + total size.
  3. PUTs the hourly files to @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES
     (PARALLEL=8, OVERWRITE=TRUE, AUTO_COMPRESS=FALSE).
  4. PUTs any metadata files to @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES
     (same flags). Skipped silently if no meta files exist for the year.
  5. LISTs the stage contents for the year (post-upload verification).
  6. Reports rows already in STAGING.stg_pems_hour_raw for the given year so
     you can sanity-check the COPY result after the DAG ingests the staged
     files.

REQUIREMENTS
  - snowsql installed and the "pems" connection configured in ~/.snowsql/config
    (account, user, role, warehouse=TRAFFIC_PEMS_WH, database=TRAFFIC_PEMS_DB).
  - Source data present under:
      <repo>/data/station-hour/d11_text_station_hour_<year>_*.txt.gz
      <repo>/data/metadata/d11_text_meta_<year>_*.txt   (optional)

EXAMPLES
  # Standard interactive run — prompts for password once.
  ./scripts/r3_backfill_year.sh 2023

  # Unattended (e.g. CI) — pass the password via env var, no prompt.
  SNOWSQL_PWD='********' ./scripts/r3_backfill_year.sh 2024

  # Tee output to a logfile for audit.
  ./scripts/r3_backfill_year.sh 2023 2>&1 | tee logs/backfill_2023.log

NEXT STEPS (after this script finishes)
  - Trigger the `pems_traffic_pipeline` DAG (Airflow) so it COPYs the staged
    files into STAGING.stg_pems_hour_raw and runs the dimension + fact loads.
  - Re-run this script for additional years as needed.

EXIT CODES
  0   success
  1   bad usage, missing year, or empty password
  *   first non-zero exit from snowsql or any pipeline step (set -e)
EOF
}

# --- Argument parsing ---
case "${1:-}" in
  -h|--help|help)
    usage
    exit 0
    ;;
  '')
    echo "error: missing <year> argument" >&2
    echo "" >&2
    usage >&2
    exit 1
    ;;
esac

if [[ ! "$1" =~ ^[0-9]{4}$ ]]; then
  echo "error: <year> must be a 4-digit number (got: $1)" >&2
  echo "" >&2
  usage >&2
  exit 1
fi
YEAR="$1"

# Prompt for the Snowflake password once and export it so all snowsql calls
# pick it up automatically. SnowSQL reads $SNOWSQL_PWD natively, so we never
# pass it on the command line (where it'd show up in `ps`) or hardcode it.
if [[ -z "${SNOWSQL_PWD:-}" ]]; then
  # /dev/tty so the prompt still works when stdout is piped/redirected.
  printf 'Snowflake password for connection "pems": ' > /dev/tty
  IFS= read -rs SNOWSQL_PWD < /dev/tty
  printf '\n' > /dev/tty
  if [[ -z "$SNOWSQL_PWD" ]]; then
    echo "error: empty password" >&2
    exit 1
  fi
fi
export SNOWSQL_PWD
# Belt-and-braces: scrub the password from the environment when the script
# exits (normal or error) so it doesn't linger in child-process env dumps.
trap 'unset SNOWSQL_PWD' EXIT
REPO=/Users/epatlan/Documents/SDSU/Terms/2026/Spring2026/project/snowflake
DATA_HOUR="$REPO/data/station-hour"
DATA_META="$REPO/data/metadata"

RUN_START=$(date +%s)
log "==== PeMS Snowflake ingestion — year=$YEAR ===="
log "repo=$REPO"
log "stages: @STG_PEMS_FILES (hourly), @STG_PEMS_META_FILES (metadata)"

# ---------- Hourly files ----------
log_step "Scanning hourly files for $YEAR"
shopt -s nullglob
hour_files=("$DATA_HOUR"/d11_text_station_hour_"${YEAR}"_*.txt.gz)
shopt -u nullglob
hour_count=${#hour_files[@]}
if [[ $hour_count -eq 0 ]]; then
  log "    no hourly files matched ${DATA_HOUR}/d11_text_station_hour_${YEAR}_*.txt.gz — nothing to upload"
else
  hour_bytes=$(du -ck "${hour_files[@]}" | tail -1 | awk '{print $1}')
  log "    found $hour_count file(s), total ${hour_bytes} KiB"
  log_step "PUT hourly files → @STG_PEMS_FILES (PARALLEL=8, OVERWRITE=TRUE)"
  t0=$(date +%s)
  snowsql -c pems -o friendly=false -o timing=true -q "PUT file://${DATA_HOUR}/d11_text_station_hour_${YEAR}_*.txt.gz \
    @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES \
    AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE;"
  t1=$(date +%s)
  log "    hourly PUT done in $(elapsed $t0 $t1)"
fi

# ---------- Meta files ----------
log_step "Scanning meta files for $YEAR"
shopt -s nullglob
meta_files=("$DATA_META"/d11_text_meta_"${YEAR}"_*.txt)
shopt -u nullglob
meta_count=${#meta_files[@]}
if [[ $meta_count -gt 0 ]]; then
  meta_bytes=$(du -ck "${meta_files[@]}" | tail -1 | awk '{print $1}')
  log "    found $meta_count meta file(s), total ${meta_bytes} KiB"
  log_step "PUT meta files → @STG_PEMS_META_FILES (PARALLEL=8, OVERWRITE=TRUE)"
  t0=$(date +%s)
  snowsql -c pems -o friendly=false -o timing=true -q "PUT file://${DATA_META}/d11_text_meta_${YEAR}_*.txt \
    @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES \
    AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE;"
  t1=$(date +%s)
  log "    meta PUT done in $(elapsed $t0 $t1)"
else
  log "    no meta files for $YEAR — skipping"
fi

# ---------- Verification ----------
log_step "Stage contents for $YEAR (hourly)"
snowsql -c pems -o friendly=false -o timing=true -q "LIST @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES PATTERN='.*${YEAR}_.*';"

log_step "Raw row count already loaded for $YEAR (post-COPY sanity)"
snowsql -c pems -o friendly=false -o timing=true -q "
  SELECT COUNT(*) AS raw_rows_for_${YEAR}
    FROM TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw
   WHERE YEAR(sample_datetime) = ${YEAR};"

RUN_END=$(date +%s)
log "==== Done. Total elapsed: $(elapsed $RUN_START $RUN_END). Year=$YEAR ===="
log "Next: trigger the pems_traffic_pipeline DAG (or run COPY INTO + procs manually) to ingest the staged files."
