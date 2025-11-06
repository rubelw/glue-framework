#!/usr/bin/env bash
#
# scripts/setup_infra.sh
# Bootstraps local infra:
# - Ensures AWS CLI v2 is installed
# - Configures MinIO-backed local S3
# - Ensures warehouse bucket exists
# - Optionally populates DB2 with data from /data directory
#
# Usage:
#   ./scripts/setup_infra.sh
#   LOCAL_S3_PROFILE=local-s3 MINIO_ENDPOINT=http://localhost:9000 ./scripts/setup_infra.sh
#   ./scripts/setup_infra.sh --profile local-s3 --endpoint http://localhost:9000 --bucket warehouse
#

set -euo pipefail

# ---- Defaults ----
LOCAL_S3_PROFILE="${LOCAL_S3_PROFILE:-local-s3}"
LOCAL_S3_ACCESS_KEY="${LOCAL_S3_ACCESS_KEY:-minioadmin}"
LOCAL_S3_SECRET_KEY="${LOCAL_S3_SECRET_KEY:-minioadmin}"
LOCAL_S3_REGION="${LOCAL_S3_REGION:-us-east-1}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-warehouse}"

# DB2 defaults
DB2_CONTAINER_NAME="${DB2_CONTAINER_NAME:-}"
DB2_PORT="${DB2_PORT:-50000}"
DB2_INSTANCE="${DB2_INSTANCE:-db2inst1}"
DB2_PASSWORD="${DB2_PASSWORD:-password}"
DB2_DBNAME="${DB2_DBNAME:-TESTDB}"
DATA_DIR="${DATA_DIR:-data}"

# Preview rows per table after load (tweak if needed)
SHOW_ROWS_LIMIT="${SHOW_ROWS_LIMIT:-100}"

# ---- Logging ----
log()  { printf '%s %s\n' "[INFO]" "$*"; }
warn() { printf '%s %s\n' "[WARN]" "$*" >&2; }
err()  { printf '%s %s\n' "[ERROR]" "$*" >&2; }

# --- DB2 helpers (CLP-safe, no SQL PL blocks) -------------------------------

# Execute a DB2 CLP command as the instance user inside container
# Usage: db2_exec <container> <instance> "<single SQL/CLP string>"
db2_exec() {
  local cid="$1" inst="$2" cmd="$3"
  docker exec -e LANG=C -e LC_ALL=C "$cid" bash -lc "
    set -e
    su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 -v \"$cmd\"
    '
  "
}

# Execute a statement while connected to a DB, then reset
# Usage: db2_exec_in_db <container> <instance> <db> "<single SQL/CLP string>"
db2_exec_in_db() {
  local cid="$1" inst="$2" db="$3" cmd="$4"
  docker exec -e LANG=C -e LC_ALL=C "$cid" bash -lc "
    set -e
    su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 -v \"$cmd\"
      db2 connect reset >/dev/null
    '
  "
}

# Return a single scalar from DB2 with a guaranteed connection
# Usage: db2_scalar_in_db <container> <instance> <db> "<SQL>"
db2_scalar_in_db() {
  local cid="$1" inst="$2" db="$3" sql="$4"
  docker exec -e LANG=C -e LC_ALL=C "$cid" bash -lc "
    su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 -x \"$sql\" 2>/dev/null | tr -d \"[:space:]\"
    '
  "
}

# Ensure schema exists (ignore errors if it already exists)
db2_ensure_schema() {
  local cid="$1" inst="$2" db="$3" sch
  sch="$(echo "$4" | tr '[:lower:]' '[:upper:]')"
  db2_exec "$cid" "$inst" "connect to ${db}" || true
  db2_exec "$cid" "$inst" "CREATE SCHEMA ${sch}" || true
  db2_exec "$cid" "$inst" "connect reset" || true
}

# Create table if missing, mapping provided CSV header to VARCHAR(512) cols
db2_create_table_if_missing() {
  # $1: container, $2: inst, $3: db, $4: schema, $5: table, $6: csv_header (comma-separated)
  local cid="$1" inst="$2" db="$3" sch tbl header
  sch="$(echo "$4" | tr '[:lower:]' '[:upper:]')"
  tbl="$(echo "$5" | tr '[:lower:]' '[:upper:]')"
  header="$6"

  db2_exec "$cid" "$inst" "connect to ${db}" >/dev/null

  local cnt
  cnt="$(db2_scalar_in_db "$cid" "$inst" "$db" "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA='${sch}' AND TABNAME='${tbl}'")"
  cnt="${cnt:-0}"

  if [ "$cnt" = "0" ]; then
    local cols
    cols="$(
      echo "$header" | awk -F',' '
        {
          for (i=1;i<=NF;i++) {
            gsub(/[^A-Za-z0-9_]/,"",$i);
            if ($i == "") $i=sprintf("COL_%d", i);
            printf("%s VARCHAR(512)%s", toupper($i), (i<NF?", ":""))
          }
        }'
    )"
    db2_exec "$cid" "$inst" "CREATE TABLE ${sch}.${tbl} (${cols})"
  fi

  db2_exec "$cid" "$inst" "connect reset" >/dev/null
}

# Load a single CSV file into schema.table (REPLACE keeps table shape stable)
db2_import_csv_replace() {
  # $1: container, $2: inst, $3: db, $4: schema, $5: table, $6: /path/in/container.csv (NO HEADER)
  local cid="$1" inst="$2" db="$3" sch="$4" tbl="$5" inpath="$6"
  docker exec -e LANG=C -e LC_ALL=C "$cid" bash -lc "
    set -e
    su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 \"IMPORT FROM ${inpath} OF DEL MODIFIED BY COLDEL, CODEPAGE=1208 COMMITCOUNT 1000 REPLACE INTO ${sch}.${tbl}\"
      rc=\$?
      db2 connect reset >/dev/null
      if [ \$rc -ne 0 ] && [ \$rc -ne 4 ]; then exit \$rc; fi
    '
  "
}

# Does table exist? 0 if exists, 1 otherwise
db2_table_exists() {
  local cid="$1" inst="$2" db="$3" sch="$4" tbl="$5"
  docker exec -e LANG=C -e LC_ALL=C "$cid" bash -lc "
    su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 \"DESCRIBE TABLE ${sch}.${tbl}\" >/dev/null 2>&1
      rc=\$?
      db2 connect reset >/dev/null
      exit \$rc
    '
  " >/dev/null 2>&1
}

# Load a single CSV into Db2 (CREATE-IF-MISSING + IMPORT, delimiter autodetect)
# Usage: load_csv <container> <db_name> <schema> <table> <csv_abs_path_in_container>
load_csv() {
  local cid="$1" db="$2" schema="$3" table="$4" csv_abs="$5"

  local tmp_dir="/tmp/seed"
  local tmp_nohdr="${tmp_dir}/nohdr_${schema}_${table}.csv"

  echo "[INFO] Loading table '${schema}.${table}' from $(basename "$csv_abs")"

  # Workspace + strip header (inside container)
  docker exec "$cid" bash -lc "
    set -e
    mkdir -p '$tmp_dir'
    chmod 1777 '$tmp_dir'
    [ -f '$csv_abs' ] || { echo '[ERROR] Missing CSV: $csv_abs' >&2; exit 2; }
    tail -n +2 '$csv_abs' > '$tmp_nohdr'
  "

  # Detect delimiter from first data line
  local sample_line mod_clause
  sample_line="$(docker exec "$cid" bash -lc "head -n1 '$tmp_nohdr' | tr -d '\r'")"
  if   [[ "$sample_line" == *$'\t'* ]]; then
    mod_clause="COLDELx'09' CODEPAGE=1208 COMMITCOUNT 1000"
  elif [[ "$sample_line" == *","* ]]; then
    mod_clause="COLDEL, CODEPAGE=1208 COMMITCOUNT 1000"
  elif [[ "$sample_line" == *";"* ]]; then
    mod_clause="COLDEL; CODEPAGE=1208 COMMITCOUNT 1000"
  elif [[ "$sample_line" == *"|"* ]]; then
    mod_clause="COLDEL| CODEPAGE=1208 COMMITCOUNT 1000"
  else
    mod_clause="COLDEL, CODEPAGE=1208 COMMITCOUNT 1000"
  fi

  # Build column list from header (normalize)
  local header lc_header sane_header cols_sql
  header="$(docker exec "$cid" bash -lc "head -n1 '$csv_abs' | tr -d '\r'")"
  lc_header="$(printf '%s' "$header" | tr '[:upper:]' '[:lower:]')"
  sane_header="$(printf '%s' "$lc_header" | sed -E 's/[^a-z0-9_,]/_/g')"
  cols_sql="$(
    printf '%s\n' "$sane_header" | awk -F',' '
      {
        for (i=1;i<=NF;i++) {
          col=$i; if (col=="") col=sprintf("col_%d", i)
          printf("%s VARCHAR(512)%s", col, (i<NF?", ":""))
        }
      }'
  )"

  # Ensure schema and table
  if ! db2_exec_in_db "$cid" "${DB2_INSTANCE}" "${db}" "CREATE SCHEMA ${schema}"; then :; fi
  if ! db2_table_exists "$cid" "${DB2_INSTANCE}" "${db}" "${schema}" "${table}"; then
    echo "[INFO] Creating ${schema}.${table} from CSV header…"
    if ! db2_exec_in_db "$cid" "${DB2_INSTANCE}" "${db}" "CREATE TABLE ${schema}.${table} (${cols_sql})"; then
      echo "[WARN] CREATE TABLE returned non-zero. Proceeding with import."
    fi
  fi

  # IMPORT (header removed). Accept rc 0 or 4.
  docker exec -e LANG=C -e LC_ALL=C "$cid" bash -lc "
    set -e
    inst=\${DB2_INSTANCE:-db2inst1}
    su - \${inst} -s /bin/bash -lc '
      set -e
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 \"IMPORT FROM ${tmp_nohdr} OF DEL MODIFIED BY ${mod_clause} INSERT INTO ${schema}.${table}\"
      rc=\$?
      db2 connect reset >/dev/null
      [ \$rc -eq 0 -o \$rc -eq 4 ]
    '
  " || {
    echo "[ERROR] IMPORT failed for ${schema}.${table}. Dumping last db2diag…"
    docker exec "$cid" bash -lc "test -f ~\${DB2_INSTANCE:-db2inst1}/sqllib/db2dump/db2diag.log && tail -n 200 ~\${DB2_INSTANCE:-db2inst1}/sqllib/db2dump/db2diag.log || true"
    return 1
  }

  echo "[OK] Loaded $(basename "$csv_abs") into ${schema}.${table}"
}

# ---- Resolve & perms ----
resolve_db2_container() {
  if [[ -n "${DB2_CONTAINER_NAME}" ]]; then
    if docker ps --format '{{.Names}}' | grep -q "^${DB2_CONTAINER_NAME}$"; then
      echo "${DB2_CONTAINER_NAME}"
      return 0
    fi
  fi
  local c
  c=$(docker ps --filter "label=com.docker.compose.service=db2" --format '{{.Names}}' | head -n1 || true)
  if [[ -n "${c:-}" ]]; then echo "$c"; return 0; fi
  c=$(docker ps --filter "ancestor=icr.io/db2_community/db2" --format '{{.Names}}' | head -n1 || true)
  if [[ -n "${c:-}" ]]; then echo "$c"; return 0; fi
  c=$(docker ps --format '{{.Names}}' | grep -i 'db2' | head -n1 || true)
  if [[ -n "${c:-}" ]]; then echo "$c"; return 0; fi
  echo ""; return 1
}

ensure_db2_permissions() {
  local container="$1"
  docker exec -i "$container" bash -lc '
    set -euo pipefail
    for d in /database /database/config; do
      [ -d "$d" ] && chmod 755 "$d" || true
    done
    if [ -d /database/config/'"$DB2_INSTANCE"' ]; then
      chown -R '"$DB2_INSTANCE"':db2iadm1 /database/config/'"$DB2_INSTANCE"' || true
    fi
  ' >/dev/null 2>&1 || true
}

# --- Normalize CSV -> table name
# Rules:
#  - If path looks like "<entity>/<env>.csv" under DATA_DIR, use "<ENTITY>_<ENV>"
#  - Otherwise:
#      * use the first two path components under DATA_DIR (if present)
#      * ignore the file stem if it duplicates the last directory segment
#      * sanitize to [A-Za-z0-9_], UPPERCASE for Db2
# Examples:
#   data/customers/prod.csv        -> CUSTOMERS_PROD
#   data/customers/dev.csv         -> CUSTOMERS_DEV
#   data/orders/dev.csv            -> ORDERS_DEV
#   data/payments/dev.csv          -> PAYMENTS_DEV
#   data/customers/dev/customers.csv -> CUSTOMERS_DEV
#   data/customers/dev/customers_part1.csv -> CUSTOMERS_DEV
infer_table_from_path() {
  # Input: absolute or DATA_DIR-relative csv path
  # Output: CANONICAL UPPERCASE table name like CUSTOMERS_PROD, ORDERS_DEV, PAYMENTS_DEV
  local csv_local="$1" relpath fname stem dirpath entity env tbl
  relpath="${csv_local#${DATA_DIR%/}/}"
  fname="$(basename "$relpath")"
  stem="${fname%.[cC][sS][vV]}"
  dirpath="$(dirname "$relpath")"

  # split directories under DATA_DIR
  IFS='/' read -r -a parts <<< "$dirpath"
  entity="${parts[0]:-}"
  env="${parts[1]:-}"

  # lowercase copies for comparisons
  lc_entity="$(printf '%s' "$entity" | tr '[:upper:]' '[:lower:]')"
  lc_env="$(printf '%s' "$env" | tr '[:upper:]' '[:lower:]')"
  lc_stem="$(printf '%s' "$stem" | tr '[:upper:]' '[:lower:]')"

  # env whitelist
  case "$lc_env" in
    dev|prod|stage|staging|test|qa|uat) env_ok=1 ;;
    *) env_ok=0 ;;
  esac

  if [ -n "$entity" ] && [ $env_ok -eq 1 ]; then
    # canonical: first two dirs only
    tbl="${entity}_${env}"
  elif [ -n "$entity" ] && [ -n "$env" ]; then
    # if second dir isn't a known env, still use first two dirs
    tbl="${entity}_${env}"
  elif [ -n "$entity" ]; then
    # only one dir present -> use dir + (optional) stem if stem != entity
    if [ "$lc_stem" = "$lc_entity" ]; then
      tbl="${entity}"
    else
      tbl="${entity}_${stem}"
    fi
  else
    # CSV directly under DATA_DIR
    tbl="${stem}"
  fi

  # If stem equals ANY of the chosen components, drop it (prevents *_DEV_PAYMENTS)
  if [ "$lc_stem" = "$lc_entity" ] || [ "$lc_stem" = "$lc_env" ]; then
    : # do nothing; stem already represented
  fi

  # sanitize and uppercase
  tbl="$(printf '%s' "$tbl" | sed 's/[^A-Za-z0-9_]/_/g; s/__*/_/g; s/^_//; s/_$//')"
  printf '%s\n' "$(printf '%s' "$tbl" | tr '[:lower:]' '[:upper:]')"
}



# Drop legacy tables that don't match desired patterns (optional)
db2_drop_legacy_tables() {
  local container="$(resolve_db2_container || true)"
  [[ -z "$container" ]] && return 0
  local inst="${DB2_INSTANCE:-db2inst1}"
  local db="${DB2_DBNAME:-TESTDB}"
  local schema_u; schema_u="$(printf '%s' "$inst" | tr '[:lower:]' '[:upper:]')"

  # keep patterns you want; drop the rest that match these unwanted ones
  local -a patterns_to_drop=(
    '^DEV$' '^PROD$' '.*_CUSTOMERS_PART[0-9]+$' '.*_ORDERS_PART[0-9]+$'
  )

  docker exec -i "$container" bash -lc "
    su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 \"SET CURRENT SCHEMA ${schema_u}\" >/dev/null
      tables=\$(db2 -x \"SELECT RTRIM(TABNAME) FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT SCHEMA\")
      for t in \$tables; do
        keep=1
        for rx in ${patterns_to_drop[*]}; do
          echo \"\$t\" | egrep -q \"\$rx\" && keep=0
        done
        if [ \"\$keep\" -eq 0 ]; then
          echo \"[INFO] Dropping legacy table ${schema_u}.\$t\"
          db2 \"DROP TABLE \\\"${schema_u}\\\".\\\"\$t\\\"\" >/dev/null 2>&1 || true
        fi
      done
      db2 connect reset >/dev/null
    '
  " || true
}

# --- Load all CSVs (case-insensitive discovery) ---
populate_db2() {
  local container
  container="$(resolve_db2_container || true)"
  if [[ -z "${container}" ]]; then
    warn "DB2 container not found — skipping DB2 data load."
    return
  fi
  log "Using DB2 container: ${container}"

  log "Waiting for DB2 at port ${DB2_PORT} to be ready..."
  for i in {1..60}; do
    if (echo > /dev/tcp/127.0.0.1/${DB2_PORT}) >/dev/null 2>&1; then
      log "DB2 port is open."
      break
    fi
    sleep 2
  done

  ensure_db2_permissions "${container}"

  local schema_u
  schema_u="$(printf '%s' "${DB2_INSTANCE:-db2inst1}" | tr '[:lower:]' '[:upper:]')"

  log "Populating DB2 database '${DB2_DBNAME}' from '${DATA_DIR}/'..."
  # --- NEW: case-insensitive discovery + scan logging (handles trailing spaces after .csv) ---
  log "Scanning '${DATA_DIR}/' for CSVs (case-insensitive)…"
  local -a all_csvs=()
  # Use -iregex to match .csv followed by optional whitespace (BSD/macOS find compatible)
  while IFS= read -r -d '' f; do
    all_csvs+=("$f")
  done < <(LC_ALL=C find "${DATA_DIR}" -type f \( -iregex '.*\.csv[[:space:]]*' \) -print0 2>/dev/null || true)

  if (( ${#all_csvs[@]} == 0 )); then
    warn "No CSV files found under '${DATA_DIR}/' — skipping DB2 population."
    return
  fi

  log "Found ${#all_csvs[@]} CSV file(s):"
  # Quote filenames so weird characters (like trailing spaces) are visible in logs
  for __p in "${all_csvs[@]}"; do
    printf "  - '%s'\n" "$__p"
  done

  # --- debug: confirm expected orders path exists & list contents
  if [ -d "${DATA_DIR%/}/orders/dev" ]; then
    log "Debug: listing '${DATA_DIR%/}/orders/dev' (case shown exactly):"
    # -lb shows escapes for odd characters/whitespace; helps catch 'orders_part1.csv '
    ( ls -lb "${DATA_DIR%/}/orders/dev" || true )
  else
    warn "Debug: '${DATA_DIR%/}/orders/dev' directory not found."
  fi

  # --- end discovery/logging ---

  local loaded=0 failed=0

  for csv_local in "${all_csvs[@]}"; do
    # Canonical table name (e.g., CUSTOMERS_DEV, ORDERS_DEV, PAYMENTS_DEV)
    local tbl_u
    tbl_u="$(infer_table_from_path "$csv_local")"

    local in_container="/tmp/seed/${tbl_u}.csv"
    docker exec -i "${container}" bash -lc "mkdir -p /tmp/seed"
    docker cp "${csv_local}" "${container}:${in_container}"

    # pretty relative path for logs when possible
    local rel_disp
    rel_disp="$(realpath --relative-to="${DATA_DIR}" "$csv_local" 2>/dev/null || echo "$csv_local")"
    log "Mapping '${rel_disp}' -> '${schema_u}.${tbl_u}'"

    if ! load_csv "${container}" "${DB2_DBNAME}" "${schema_u}" "${tbl_u}" "${in_container}"; then
      err "Failed loading $(basename "$csv_local") into ${schema_u}.${tbl_u}"
      failed=$((failed + 1))
    else
      loaded=$((loaded + 1))
    fi
  done

  log "CSV load summary: loaded=${loaded}, failed=${failed}"
  # Never fail overall setup because some files failed to load
  return 0
}

db2_canonicalize_tables() {
  local container; container="$(resolve_db2_container || true)"
  [[ -z "$container" ]] && return 0
  local inst="${DB2_INSTANCE:-db2inst1}"
  local db="${DB2_DBNAME:-TESTDB}"
  local schema_u; schema_u="$(printf '%s' "$inst" | tr '[:lower:]' '[:upper:]')"

  docker exec -i "$container" bash -lc "
    su - ${inst} -s /bin/bash -lc '
      set -eo pipefail
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null
      db2 \"SET CURRENT SCHEMA ${schema_u}\" >/dev/null

      tables=\$(db2 -x \"SELECT RTRIM(TABNAME) FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT SCHEMA\")
      for t in \$tables; do
        T_UP=\$(echo \"\$t\" | tr \"[:lower:]\" \"[:upper:]\")
        # Drop duplicates like FOO_DEV_FOO, FOO_PROD_FOO, etc.
        if echo \"\$T_UP\" | grep -Eq \"^[A-Z0-9_]+_(DEV|PROD|STAGE|STAGING|TEST|QA|UAT)_[A-Z0-9_]+$\"; then
          base=\$(echo \"\$T_UP\" | sed -E \"s/^([A-Z0-9_]+)_(DEV|PROD|STAGE|STAGING|TEST|QA|UAT)_.+$/\\1_\\2/\")
          exists=\$(db2 -x \"SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA=CURRENT SCHEMA AND TABNAME='\$base'\" | tr -d '[:space:]')
          if [ \"\$exists\" = \"1\" ]; then
            echo \"[INFO] Dropping non-canonical table ${schema_u}.\$t\"
            db2 \"DROP TABLE \\\"${schema_u}\\\".\\\"\$t\\\"\" >/dev/null 2>&1 || true
          fi
        fi
      done

      db2 connect reset >/dev/null
    '
  " || true
}

dump_db2_tables_and_rows() {
  # Iterate on HOST; each table is best-effort and cannot kill the run
  set +u
  local container; container="$(resolve_db2_container || true)"
  if [[ -z "${container:-}" ]]; then err "DB2 container not found"; set -u; return 0; fi

  local inst="${DB2_INSTANCE:-db2inst1}"
  local db="${DB2_DBNAME:-TESTDB}"
  local schema_u; schema_u="$(printf '%s' "$inst" | tr '[:lower:]' '[:upper:]')"
  local limit="${SHOW_ROWS_LIMIT:-100}"

  log "Discovering tables in schema ${schema_u}…"
  log "[DEBUG] Using SCHEMA_U='${schema_u}'"

  # Get raw names (Db2 -x returns fixed-width padded columns)
  local tables_raw
  tables_raw="$(
    { docker exec -e SCHEMA_U="$schema_u" -i "$container" bash -lc '
        set -eo pipefail
        su - '"$inst"' -s /bin/bash -lc "
          source ~/sqllib/db2profile >/dev/null 2>&1 || true
          db2 connect to '"$db"' >/dev/null
          db2 -x \"SELECT TABNAME
                   FROM SYSCAT.TABLES
                   WHERE TABSCHEMA = UPPER('\''$SCHEMA_U'\'')
                   ORDER BY TABNAME\"
          db2 connect reset >/dev/null
        "
      ' ; } 2>/dev/null || true
  )"

  log "[DEBUG] SYSCAT.TABLES printable (raw lines):"
  if [[ -n "$tables_raw" ]]; then
    printf '%s\n' "$tables_raw" | sed 's/^/  - /'
  else
    echo "  (empty)"
  fi
  log "[DEBUG] SYSCAT.TABLES hex (raw):"
  if [[ -n "$tables_raw" ]]; then
    printf '%s' "$tables_raw" | hexdump -C | sed 's/^/  /'
  else
    echo "  (empty)"
  fi

  # Normalize & filter
  local tables
  tables="$(
    printf '%s' "$tables_raw" \
      | tr -d $'\r' \
      | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' \
      | awk 'NF{print}' \
      | awk '/^[A-Z0-9_]+$/ && $0 !~ /_PART[0-9]+$/ && $0 !~ /_(DEV|PROD|STAGE|STAGING|TEST|QA|UAT)_.+$/ {print}' \
      | sort -u
  )"

  if [[ -z "${tables:-}" ]]; then
    warn "No tables found in schema ${schema_u} after normalization."
    docker exec -e SCHEMA_U="$schema_u" -i "$container" bash -lc '
      su - '"$inst"' -s /bin/bash -lc "
        source ~/sqllib/db2profile >/dev/null 2>&1 || true
        db2 connect to '"$db"' >/dev/null
        echo \"[DEBUG] COUNT(*) by TABSCHEMA:\"
        db2 -x \"SELECT TABSCHEMA, COUNT(*) FROM SYSCAT.TABLES GROUP BY TABSCHEMA ORDER BY 2 DESC\"
        db2 connect reset >/dev/null
      "
    ' || true
    set -u
    return 0
  fi

  log "Tables found:"
  printf '%s\n' "$tables" | awk 'NF{printf("  - %s\n",$0)}'
  echo
  log "Dumping up to ${limit} rows per table…"

  set +e
  while IFS= read -r TBL; do
    [[ -z "${TBL:-}" ]] && continue
    printf '\n=== BEGIN %s ===\n' "$TBL"

    # Row count
    local cnt
    cnt="$(
      docker exec -e SCHEMA_U="$schema_u" -e TBL="$TBL" -i "$container" bash -lc '
        set -e
        su - '"$inst"' -s /bin/bash -lc "
          source ~/sqllib/db2profile >/dev/null 2>&1 || true
          db2 connect to '"$db"' >/dev/null
          db2 \"SET CURRENT SCHEMA $SCHEMA_U\" >/dev/null
          db2 -x \"SELECT COUNT(*) FROM \\\"$TBL\\\"\"
          db2 connect reset >/dev/null
        "
      ' 2>/dev/null | tr -d "[:space:]"
    )"
    cnt="${cnt:-0}"
    echo "rows: ${cnt}"

    # Column list
    local header
    header="$(
      docker exec -e SCHEMA_U="$schema_u" -e TBL="$TBL" -i "$container" bash -lc '
        set -e
        su - '"$inst"' -s /bin/bash -lc "
          source ~/sqllib/db2profile >/dev/null 2>&1 || true
          db2 connect to '"$db"' >/dev/null
          db2 \"SET CURRENT SCHEMA $SCHEMA_U\" >/dev/null
          db2 -x \"SELECT RTRIM(COLNAME)
                   FROM SYSCAT.COLUMNS
                   WHERE TABSCHEMA = CURRENT SCHEMA
                     AND TABNAME   = UPPER('\''$TBL'\'')
                   ORDER BY COLNO\"
          db2 connect reset >/dev/null
        "
      ' 2>/dev/null | tr -d $'\r' | awk 'NF{print}' | paste -sd',' - 2>/dev/null
    )"
    echo "columns: ${header:-<no columns>}"

    # First N records
    echo "records (up to ${limit}):"
    docker exec -e SCHEMA_U="$schema_u" -e TBL="$TBL" -e LIMIT="$limit" -i "$container" bash -lc '
      set -e
      su - '"$inst"' -s /bin/bash -lc "
        source ~/sqllib/db2profile >/dev/null 2>&1 || true
        db2 connect to '"$db"' >/dev/null
        db2 \"SET CURRENT SCHEMA $SCHEMA_U\" >/dev/null
        db2 -x \"SELECT * FROM \\\"$TBL\\\" FETCH FIRST $LIMIT ROWS ONLY\"
        db2 connect reset >/dev/null
      "
    ' 2>/dev/null || echo "[WARN] Select failed for $TBL (continuing)"

    printf '=== END %s ===\n' "$TBL"
  done <<< "$tables"
  set -e

  # Extra: explicit ORDERS_DEV sanity dump if present
  if printf '%s\n' "$tables" | grep -qx "ORDERS_DEV"; then
    echo
    log "Sanity: explicit dump of ORDERS_DEV"
    docker exec -e SCHEMA_U="$schema_u" -i "$container" bash -lc '
      set -e
      su - '"$inst"' -s /bin/bash -lc "
        source ~/sqllib/db2profile >/dev/null 2>&1 || true
        db2 connect to '"$db"' >/dev/null
        db2 \"SET CURRENT SCHEMA $SCHEMA_U\" >/dev/null
        echo \"rows: \$(db2 -x \\\"SELECT COUNT(*) FROM \\\"ORDERS_DEV\\\"\\\" | tr -d '[:space:]')\"
        echo \"columns: \$(db2 -x \\\"SELECT RTRIM(COLNAME) FROM SYSCAT.COLUMNS WHERE TABSCHEMA = CURRENT SCHEMA AND TABNAME='ORDERS_DEV' ORDER BY COLNO\\\" | paste -sd, -)\"
        echo \"records (up to '"$limit"'):\"
        db2 -x \"SELECT * FROM \\\"ORDERS_DEV\\\" FETCH FIRST '"$limit"' ROWS ONLY\"
        db2 connect reset >/dev/null
      "
    ' 2>/dev/null || echo "[WARN] Explicit ORDERS_DEV dump failed."
  fi

  set -u
  return 0
}


# ---- 1. Ensure AWS CLI ----
ensure_awscli() {
  if command -v aws >/dev/null 2>&1; then
    log "AWS CLI found: $(aws --version | head -1)"
    return 0
  fi
  warn "AWS CLI not found — installing AWS CLI v2..."
  os="$(uname -s)"
  arch="$(uname -m)"
  if [[ "$os" == "Darwin" ]]; then
    if command -v brew >/dev/null 2>&1; then
      brew install awscli
    else
      err "Homebrew not found. Please install manually (https://brew.sh)."
      exit 1
    fi
  elif [[ "$os" == "Linux" ]]; then
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    if [[ "$arch" == "x86_64" || "$arch" == "amd64" ]]; then
      url="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
    elif [[ "$arch" == "aarch64" || "$arch" == "arm64" ]]; then
      url="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"
    else
      err "Unsupported arch: $arch"
      exit 1
    fi
    curl -sSL "$url" -o "$tmp_dir/awscliv2.zip"
    unzip -q "$tmp_dir/awscliv2.zip" -d "$tmp_dir"
    sudo "$tmp_dir/aws/install"
    log "AWS CLI installed."
  else
    err "Unsupported OS: $os"
    exit 1
  fi
}

# ---- 2. Setup AWS CLI profiles for MinIO ----
write_local_profile() {
  mkdir -p "${HOME}/.aws"
  cred="${HOME}/.aws/credentials"
  cfg="${HOME}/.aws/config"
  touch "$cred" "$cfg"
  if ! awk '/^\['"$LOCAL_S3_PROFILE"'\]$/{found=1} END{exit !found}' "$cred" >/dev/null 2>&1; then
    log "Writing credentials for $LOCAL_S3_PROFILE"
    {
      echo
      echo "[$LOCAL_S3_PROFILE]"
      echo "aws_access_key_id=${LOCAL_S3_ACCESS_KEY}"
      echo "aws_secret_access_key=${LOCAL_S3_SECRET_KEY}"
    } >> "$cred"
  else
    log "Credentials profile '$LOCAL_S3_PROFILE' already present"
  fi
  if ! awk '/^\[profile '"$LOCAL_S3_PROFILE"'\]$/{found=1} END{exit !found}' "$cfg" >/dev/null 2>&1; then
    log "Writing config for $LOCAL_S3_PROFILE"
    {
      echo
      echo "[profile ${LOCAL_S3_PROFILE}]"
      echo "region = ${LOCAL_S3_REGION}"
      echo "output = json"
      echo "s3 ="
      echo "    endpoint_url = ${MINIO_ENDPOINT}"
      echo "    addressing_style = path"
    } >> "$cfg"
  else
    log "Config profile '$LOCAL_S3_PROFILE' already present"
  fi
}

# ---- 3. Verify MinIO connectivity and bucket ----
check_local_s3() {
  log "Checking MinIO connectivity at ${MINIO_ENDPOINT}..."
  if ! aws --profile "${LOCAL_S3_PROFILE}" --endpoint-url "${MINIO_ENDPOINT}" s3 ls >/dev/null 2>&1; then
    err "MinIO not reachable at ${MINIO_ENDPOINT}. Try: make start"
    exit 1
  fi
  log "MinIO reachable."
}

ensure_warehouse_bucket() {
  if ! aws --profile "${LOCAL_S3_PROFILE}" --endpoint-url "${MINIO_ENDPOINT}" s3 ls "s3://${WAREHOUSE_BUCKET}" >/dev/null 2>&1; then
    log "Creating bucket s3://${WAREHOUSE_BUCKET}"
    aws --profile "${LOCAL_S3_PROFILE}" --endpoint-url "${MINIO_ENDPOINT}" s3 mb "s3://${WAREHOUSE_BUCKET}"
  else
    log "Bucket already exists: s3://${WAREHOUSE_BUCKET}"
  fi
}

# ---- 5. Main ----
main() {
  log "Setup parameters:"
  echo "  profile    = ${LOCAL_S3_PROFILE}"
  echo "  endpoint   = ${MINIO_ENDPOINT}"
  echo "  db2 name   = ${DB2_DBNAME}"
  echo "  data dir   = ${DATA_DIR}"
  echo "  show rows  = ${SHOW_ROWS_LIMIT}"
  echo

  ensure_awscli
  write_local_profile
  check_local_s3
  ensure_warehouse_bucket

  # Load data (this should be hard-fail if truly broken)
  populate_db2 || true

  # Post-load steps are best-effort; never abort the script because of them
  set +e
  db2_canonicalize_tables || true
  db2_drop_legacy_tables  || true
  dump_db2_tables_and_rows || true
  set -e

  log "DONE. MinIO + DB2 infrastructure ready."
}

main "$@"
