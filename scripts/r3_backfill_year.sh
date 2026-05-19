#!/bin/bash
# r3_backfill_year.sh — Upload one or more years of D11 PeMS files to Snowflake stages.
# Run `./scripts/r3_backfill_year.sh --help` for full usage.
set -euo pipefail

log()       { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
log_step()  { printf '\n[%s] ==> %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
elapsed()   { local s=$1 e=$2; printf '%dm%ds' $(( (e-s)/60 )) $(( (e-s)%60 )); }

REPO=/Users/epatlan/Documents/SDSU/Terms/2026/Spring2026/project/snowflake
DATA_HOUR="$REPO/data/station-hour"
DATA_META="$REPO/data/metadata"

usage() {
  cat <<'EOF'
r3_backfill_year.sh — Upload one or more years of D11 PeMS files to Snowflake stages.

USAGE
  ./scripts/r3_backfill_year.sh <year-spec> [<year-spec> ...]
  ./scripts/r3_backfill_year.sh -h | --help

YEAR-SPEC FORMS (mix and match — duplicates auto-deduped)
  2023            Single 4-digit year
  2015-2024       Inclusive range of years
  all             All years auto-detected from data/station-hour/

ARGUMENTS
  Files matching these patterns are uploaded for each resolved year:
    data/station-hour/d11_text_station_hour_<year>_*.txt.gz  →  @STG_PEMS_FILES
    data/metadata/d11_text_meta_<year>_*.txt                 →  @STG_PEMS_META_FILES

OPTIONS
  -h, --help   Show this help and exit.

AUTHENTICATION
  The script needs the password for the SnowSQL connection profile named "pems"
  (configured in ~/.snowsql/config). It prompts ONCE at the start, hides your
  input, and exports it to SnowSQL via SNOWSQL_PWD so every subsequent snowsql
  call reuses it. The password is never written to disk and is unset from the
  environment on exit. To skip the prompt (CI / unattended), pre-export
  SNOWSQL_PWD before invoking the script.

WHAT IT DOES
  1. Resolves the requested year-specs into a sorted, deduplicated year list.
  2. Prompts for the Snowflake password once (unless SNOWSQL_PWD is set).
  3. For EACH year in the list:
       a. Logs file count + total size for hourly and meta files.
       b. PUTs hourly files  → @STG_PEMS_FILES       (PARALLEL=8, OVERWRITE=TRUE).
       c. PUTs metadata files → @STG_PEMS_META_FILES (same flags). Skipped if none.
       d. LISTs stage contents for that year (verification).
       e. Reports rows already loaded in STAGING.stg_pems_hour_raw for that year.
  4. Prints a grand-total summary (years processed, files uploaded, elapsed).

REQUIREMENTS
  - snowsql installed and the "pems" connection configured in ~/.snowsql/config
    (account, user, role, warehouse=TRAFFIC_PEMS_WH, database=TRAFFIC_PEMS_DB).
  - Source data present under:
      <repo>/data/station-hour/d11_text_station_hour_<year>_*.txt.gz
      <repo>/data/metadata/d11_text_meta_<year>_*.txt   (optional, any subset)

EXAMPLES
  # One year — same behavior as before.
  ./scripts/r3_backfill_year.sh 2023

  # Multiple discrete years.
  ./scripts/r3_backfill_year.sh 2022 2023 2024

  # A range.
  ./scripts/r3_backfill_year.sh 2020-2024

  # EVERYTHING in data/station-hour/ — single prompt, looped uploads.
  ./scripts/r3_backfill_year.sh all

  # Mix of forms (deduped automatically).
  ./scripts/r3_backfill_year.sh 2015 2020-2022 2025

  # Unattended (e.g. CI) — pass the password via env var, no prompt.
  SNOWSQL_PWD='********' ./scripts/r3_backfill_year.sh all

  # Tee output to a logfile for audit.
  ./scripts/r3_backfill_year.sh all 2>&1 | tee logs/backfill_all.log

NEXT STEPS
  - Trigger the `pems_traffic_pipeline` DAG in Airflow so it COPYs the staged
    files into STAGING.stg_pems_hour_raw and runs the dimension + fact loads.

EXIT CODES
  0   success
  1   bad usage, no matching files, or empty password
  *   first non-zero exit from snowsql or any pipeline step (set -e)
EOF
}

# ----- Argument parsing -----
if [[ $# -lt 1 ]]; then
  echo "error: missing year argument(s)" >&2
  echo "" >&2
  usage >&2
  exit 1
fi

for arg in "$@"; do
  case "$arg" in
    -h|--help|help) usage; exit 0 ;;
  esac
done

# Expand each year-spec (year | range | "all") into a raw list,
# then sort -u to dedupe. Plain arrays only — works on bash 3.2 (macOS).
YEARS_RAW=()
for arg in "$@"; do
  case "$arg" in
    all)
      shopt -s nullglob
      for f in "$DATA_HOUR"/d11_text_station_hour_*_*.txt.gz; do
        y=$(basename "$f" | sed -E 's/d11_text_station_hour_([0-9]{4})_.*/\1/')
        [[ "$y" =~ ^[0-9]{4}$ ]] && YEARS_RAW+=("$y")
      done
      shopt -u nullglob
      ;;
    [0-9][0-9][0-9][0-9])
      YEARS_RAW+=("$arg")
      ;;
    [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
      lo=${arg%-*}
      hi=${arg#*-}
      if (( lo > hi )); then
        echo "error: invalid range $arg (lo > hi)" >&2
        exit 1
      fi
      for (( y=lo; y<=hi; y++ )); do YEARS_RAW+=("$y"); done
      ;;
    *)
      echo "error: unrecognized year-spec '$arg' (expected YYYY, YYYY-YYYY, or 'all')" >&2
      echo "" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ${#YEARS_RAW[@]} -eq 0 ]]; then
  echo "error: no years resolved from arguments. Check data/station-hour/ has files matching d11_text_station_hour_<year>_*.txt.gz" >&2
  exit 1
fi

# Dedupe + sort ascending for deterministic processing order.
YEARS=()
while IFS= read -r y; do YEARS+=("$y"); done < <(printf '%s\n' "${YEARS_RAW[@]}" | sort -u)

# ----- Password prompt (once) -----
if [[ -z "${SNOWSQL_PWD:-}" ]]; then
  printf 'Snowflake password for connection "pems": ' > /dev/tty
  IFS= read -rs SNOWSQL_PWD < /dev/tty
  printf '\n' > /dev/tty
  if [[ -z "$SNOWSQL_PWD" ]]; then
    echo "error: empty password" >&2
    exit 1
  fi
fi
export SNOWSQL_PWD
trap 'unset SNOWSQL_PWD' EXIT

# ----- Per-year upload function -----
upload_year() {
  local YEAR="$1"
  local YEAR_START
  YEAR_START=$(date +%s)
  local files_uploaded=0

  log_step "===== YEAR $YEAR ====="

  # Hourly files
  shopt -s nullglob
  local hour_files=("$DATA_HOUR"/d11_text_station_hour_"${YEAR}"_*.txt.gz)
  shopt -u nullglob
  local hour_count=${#hour_files[@]}
  if [[ $hour_count -eq 0 ]]; then
    log "    [hourly] no files matched ${DATA_HOUR}/d11_text_station_hour_${YEAR}_*.txt.gz — skipping"
  else
    local hour_bytes
    hour_bytes=$(du -ck "${hour_files[@]}" | tail -1 | awk '{print $1}')
    log "    [hourly] found $hour_count file(s), total ${hour_bytes} KiB"
    log "    [hourly] PUT → @STG_PEMS_FILES (PARALLEL=8, OVERWRITE=TRUE)"
    local t0 t1
    t0=$(date +%s)
    snowsql -c pems -o friendly=false -o timing=true -q "PUT file://${DATA_HOUR}/d11_text_station_hour_${YEAR}_*.txt.gz \
      @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES \
      AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE;"
    t1=$(date +%s)
    log "    [hourly] PUT done in $(elapsed "$t0" "$t1")"
    files_uploaded=$(( files_uploaded + hour_count ))
  fi

  # Meta files
  shopt -s nullglob
  local meta_files=("$DATA_META"/d11_text_meta_"${YEAR}"_*.txt)
  shopt -u nullglob
  local meta_count=${#meta_files[@]}
  if [[ $meta_count -gt 0 ]]; then
    local meta_bytes
    meta_bytes=$(du -ck "${meta_files[@]}" | tail -1 | awk '{print $1}')
    log "    [meta]   found $meta_count meta file(s), total ${meta_bytes} KiB"
    log "    [meta]   PUT → @STG_PEMS_META_FILES (PARALLEL=8, OVERWRITE=TRUE)"
    local t0 t1
    t0=$(date +%s)
    snowsql -c pems -o friendly=false -o timing=true -q "PUT file://${DATA_META}/d11_text_meta_${YEAR}_*.txt \
      @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_META_FILES \
      AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE;"
    t1=$(date +%s)
    log "    [meta]   PUT done in $(elapsed "$t0" "$t1")"
    files_uploaded=$(( files_uploaded + meta_count ))
  else
    log "    [meta]   no meta files for $YEAR — skipping"
  fi

  # Verification
  log "    [verify] LIST @STG_PEMS_FILES PATTERN='.*${YEAR}_.*'"
  snowsql -c pems -o friendly=false -q "LIST @TRAFFIC_PEMS_DB.STAGING.STG_PEMS_FILES PATTERN='.*${YEAR}_.*';"

  log "    [verify] rows already in stg_pems_hour_raw for $YEAR"
  snowsql -c pems -o friendly=false -q "
    SELECT COUNT(*) AS raw_rows_for_${YEAR}
      FROM TRAFFIC_PEMS_DB.STAGING.stg_pems_hour_raw
     WHERE YEAR(sample_datetime) = ${YEAR};"

  local YEAR_END
  YEAR_END=$(date +%s)
  log "    [done]   year=$YEAR files_uploaded=$files_uploaded elapsed=$(elapsed "$YEAR_START" "$YEAR_END")"
  TOTAL_FILES=$(( TOTAL_FILES + files_uploaded ))
}

# ----- Main loop -----
RUN_START=$(date +%s)
TOTAL_FILES=0

log "==== PeMS Snowflake ingestion ===="
log "repo=$REPO"
log "stages: @STG_PEMS_FILES (hourly), @STG_PEMS_META_FILES (metadata)"
log "years to upload (${#YEARS[@]}): ${YEARS[*]}"

for YEAR in "${YEARS[@]}"; do
  upload_year "$YEAR"
done

RUN_END=$(date +%s)
log_step "===== GRAND TOTAL ====="
log "years processed:     ${#YEARS[@]}  (${YEARS[*]})"
log "files uploaded:      $TOTAL_FILES"
log "total elapsed:       $(elapsed "$RUN_START" "$RUN_END")"
log "next: trigger the pems_traffic_pipeline DAG to COPY INTO + load dims/facts."
