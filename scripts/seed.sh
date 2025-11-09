#!/usr/bin/env bash
set -euo pipefail

# ---- Config (env-overridable) ----
DB2_CONT="${DB2_CONT:-db2-int}"
DB2_DB="${DB2_DB:-TESTDB}"
DB2_USER="${DB2_USER:-db2inst1}"
DB2_SCHEMA="${DB2_SCHEMA:-DB2INST1}"
DATA_ROOT="${DATA_ROOT:-tests/data}"       # scans ${DATA_ROOT}/<entity>/<env>/*.csv
COL_WIDTH="${COL_WIDTH:-255}"              # default VARCHAR width for inferred columns

ts()  { date +"[%H:%M:%S]"; }
log() { echo "$(ts) $*"; }

# Bash 3.2-safe: uppercase A-Z0-9, underscores, not starting with digit
to_ident() {
  local s="$1"
  s="$(printf '%s' "$s" | sed 's/[^[:alnum:]]/_/g')"
  [ -z "$s" ] && s="COL"
  case "$s" in [0-9]* ) s="_$s" ;; esac
  printf '%s' "$s" | tr '[:lower:]' '[:upper:]'
}

# Create table from a CSV header that is ALREADY inside container at $remote_csv
create_table_from_header() {
  local remote_csv="$1" schema="$2" table="$3" width="${4:-255}"

  docker exec -u "${DB2_USER}" "${DB2_CONT}" bash -lc "
    set -euo pipefail
    source \"\$HOME/sqllib/db2profile\"

    db2 -v \"CONNECT TO ${DB2_DB}\"
    db2 -v \"SET CURRENT SCHEMA = ${schema}\"

    # If exists, skip creation
    exists=\$(db2 -x \"SELECT COUNT(*) FROM SYSCAT.TABLES
                       WHERE TABSCHEMA='${schema}'
                         AND TABNAME='${table}' WITH UR\" | tr -d '[:space:]' || true)
    if [ \"\${exists:-0}\" = \"1\" ]; then
      db2 -v TERMINATE
      exit 0
    fi

    hdr=\$(head -n1 '${remote_csv}')
    IFS=',' read -r -a cols <<< \"\$hdr\"

    COLS_SQL=''
    idx=0
    for raw in \"\${cols[@]}\"; do
      name=\$(echo \"\$raw\" | sed 's/^[[:space:]]*//;s/[[:space:]]*\$//')
      ident=\$(echo \"\$name\" | tr -cd '[:alnum:]_' | tr '[:lower:]' '[:upper:]')
      if [ -z \"\$ident\" ]; then
        idx=\$((idx+1)); ident=\"COL\$idx\"
      fi
      case \"\$ident\" in [A-Z]* ) : ;; * ) ident=\"_\$ident\" ;; esac
      if [ -z \"\$COLS_SQL\" ]; then
        COLS_SQL=\"\${ident} VARCHAR(${width})\"
      else
        COLS_SQL=\"\${COLS_SQL}, \${ident} VARCHAR(${width})\"
      fi
    done

    echo \"[INFO] Creating ${schema}.${table} from header of ${remote_csv}\"
    db2 -v \"CREATE TABLE ${schema}.${table} (\$COLS_SQL)\"
    db2 -v COMMIT
    db2 -v TERMINATE
  "
}

# Import CSV (skip header) into ${schema}.${table}
import_csv() {
  local remote_csv="$1"   # /tmp/seed/foo.csv
  local schema="$2"
  local table="$3"

  docker exec -u "${DB2_USER}" "${DB2_CONT}" bash -lc '
    set -euo pipefail
    source "$HOME/sqllib/db2profile"

    SCHEMA='"'"${schema}"'"'
    TABLE='"'"${table}"'"'
    REMOTE='"'"${remote_csv}"'"'

    db2 -v "CONNECT TO '"${DB2_DB}"'"
    db2 -v "SET CURRENT SCHEMA = ${SCHEMA}"

    # Wait for the table to be visible in SYSCAT (tolerate rc=4 warnings)
    tries=0
    while :; do
      cnt=$( { db2 -x "SELECT COUNT(*) FROM SYSCAT.TABLES
                         WHERE TABSCHEMA='''${SCHEMA}'''
                           AND TABNAME='''${TABLE}''' WITH UR" 2>/dev/null || true; } | tr -d "[:space:]" )
      if [ "${cnt:-0}" = "1" ]; then
        break
      fi
      tries=$((tries+1))
      if [ "$tries" -ge 15 ]; then
        echo "[ERROR] Table ${SCHEMA}.${TABLE} not visible after ${tries}s"
        db2 -v TERMINATE
        exit 2
      fi
      sleep 1
    done

    base=$(basename "${REMOTE}")
    dir=$(dirname  "${REMOTE}")
    nohdr="${dir}/nohdr_${base}"
    tail -n +2 "${REMOTE}" > "${nohdr}"

    echo "[INFO] IMPORT ${nohdr} -> ${SCHEMA}.${TABLE}"
    set +e
    db2 -v "IMPORT FROM ${nohdr} OF DEL MODIFIED BY codepage=1208
            INSERT INTO ${SCHEMA}.${TABLE}"
    rc=$?
    set -e
    if [ "$rc" -ne 0 ] && [ "$rc" -ne 4 ]; then
      echo "[ERROR] IMPORT failed with rc=$rc"
      db2 -v TERMINATE
      exit "$rc"
    fi

    db2 -v COMMIT
    db2 -v TERMINATE
  '
}


# Single-session: create-if-needed from header, then IMPORT (skip header)
seed_one_csv_in_db2() {
  local remote_csv="$1"   # e.g., /tmp/seed/foo.csv
  local schema="$2"       # e.g., DB2INST1
  local table="$3"        # e.g., CUSTOMERS_DEV
  local width="${4:-255}" # default column width

  docker exec -u "${DB2_USER}" "${DB2_CONT}" bash -lc "
    SCHEMA='${schema}'
    TABLE='${table}'
    REMOTE='${remote_csv}'
    WIDTH='${width}'
    set -euo pipefail
    source \"\$HOME/sqllib/db2profile\"

    db2 -v \"CONNECT TO ${DB2_DB}\"
    db2 -v \"SET CURRENT SCHEMA = \${SCHEMA}\"

    exists=\$(db2 -x \"SELECT COUNT(*) FROM SYSCAT.TABLES
                        WHERE TABSCHEMA='\${SCHEMA}'
                          AND TABNAME='\${TABLE}' WITH UR\" | tr -d '[:space:]' || true)


    if [ \"\${exists:-0}\" != \"1\" ]; then
      hdr=\$(head -n1 \"\${REMOTE}\")
      IFS=, read -r -a cols <<< \"\$hdr\"
      COLS_SQL=\"\"
      idx=0
      for raw in \"\${cols[@]}\"; do
        name=\$(echo \"\$raw\" | sed 's/^[[:space:]]*//;s/[[:space:]]*\$//')
        ident=\$(echo \"\$name\" | tr -cd '[:alnum:]_' | tr '[:lower:]' '[:upper:]')
        if [ -z \"\$ident\" ]; then idx=\$((idx+1)); ident=\"COL\$idx\"; fi
        case \"\$ident\" in [A-Z]*) : ;; *) ident=\"_\$ident\" ;; esac
        if [ -z \"\$COLS_SQL\" ]; then
          COLS_SQL=\"\${ident} VARCHAR(\${WIDTH})\"
        else
          COLS_SQL=\"\${COLS_SQL}, \${ident} VARCHAR(\${WIDTH})\"
        fi
      done
      echo \"[INFO] Creating \${SCHEMA}.\${TABLE} from header of \${REMOTE}\"
      db2 -v \"CREATE TABLE \${SCHEMA}.\${TABLE} (\${COLS_SQL})\"
      db2 -v COMMIT
    fi

    base=\$(basename \"\${REMOTE}\")
    dir=\$(dirname  \"\${REMOTE}\")
    nohdr=\"\${dir}/nohdr_\${base}\"
    tail -n +2 \"\${REMOTE}\" > \"\${nohdr}\"

    echo \"[INFO] IMPORT \${nohdr} -> \${SCHEMA}.\${TABLE}\"
    set +e
    db2 -v \"IMPORT FROM \${nohdr} OF DEL MODIFIED BY codepage=1208
            INSERT INTO \${SCHEMA}.\${TABLE}\"
    rc=\$?
    set -e
    if [ \"\$rc\" -ne 0 ] && [ \"\$rc\" -ne 4 ]; then
      echo \"[ERROR] IMPORT failed with rc=\$rc\"
      db2 -v TERMINATE
      exit \"\$rc\"
    fi

    db2 -v COMMIT
    db2 -v TERMINATE
  "
}

main() {
  log "Verifying Db2 connectivity to ${DB2_DB} in ${DB2_CONT} as ${DB2_USER}…"
  docker exec -u "${DB2_USER}" "${DB2_CONT}" bash -lc "source \$HOME/sqllib/db2profile; db2 -v CONNECT TO ${DB2_DB} >/dev/null; db2 -v TERMINATE >/dev/null"

  log "Ensuring schema ${DB2_SCHEMA} exists…"
  docker exec -u "${DB2_USER}" "${DB2_CONT}" bash -lc "
    set -euo pipefail
    source \"\$HOME/sqllib/db2profile\"
    db2 -v \"CONNECT TO ${DB2_DB}\"
    db2 -v \"CREATE SCHEMA ${DB2_SCHEMA}\" || true
    db2 -v \"SET CURRENT SCHEMA = ${DB2_SCHEMA}\"
    db2 -v COMMIT
    db2 -v TERMINATE
  "

  docker exec -u "${DB2_USER}" "${DB2_CONT}" bash -lc "mkdir -p /tmp/seed"

  # Walk all CSVs (case-insensitive) so orders/*.CSV also gets picked up
  find "${DATA_ROOT}" -type f -iname '*.csv' -print0 | sort -z |
  while IFS= read -r -d '' f; do
    echo "[FOUND] $f"
    base="$(basename "$f")"
    envdir="$(basename "$(dirname "$f")")"                    # dev/prod/…
    entity="$(basename "$(dirname "$(dirname "$f")")")"       # customers/orders/…

    ENTITY="$(to_ident "$entity")"
    ENVIRON="$(to_ident "$envdir")"
    TABLE="${ENTITY}_${ENVIRON}"

    log "Processing ${f} -> table ${DB2_SCHEMA}.${TABLE}"

    # Copy into container first (so header is available there)
    docker cp "${f}" "${DB2_CONT}:/tmp/seed/${base}"

    log "Seeding ${DB2_SCHEMA}.${TABLE} from /tmp/seed/${base}…"
    seed_one_csv_in_db2 "/tmp/seed/${base}" "${DB2_SCHEMA}" "${TABLE}" "${COL_WIDTH}"
  done
  log "Done seeding ${DB2_DB}."
}

main "$@"
