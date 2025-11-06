#!/usr/bin/env bash
# scripts/start_infra.sh
# Start MinIO and Db2 with docker compose and block until Db2 is actually usable.
# This script is intentionally verbose and defensive.

set -euo pipefail

# -----------------------------
# Tunables (override via env)
# -----------------------------
MINIO_SERVICE="${MINIO_SERVICE:-minio}"
DB2_SERVICE="${DB2_SERVICE:-db2}"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"   # MinIO API
MINIO_CONSOLE_URL="${MINIO_CONSOLE_URL:-http://localhost:9001}"

DB2_PORT_HOST="${DB2_PORT_HOST:-50000}"   # Host-mapped Db2 TCP port
DB2_INSTANCE="${DB2_INSTANCE:-db2inst1}"  # Instance user name inside container
DB2_DBNAME="${DB2_DBNAME:-TESTDB}"        # Database to ensure/auto-create
DB2_WAIT_SECS="${DB2_WAIT_SECS:-900}"     # Total patience for Db2 to initialize (seconds)

# -----------------------------
# Helpers
# -----------------------------
die() { echo "[ERROR] $*" >&2; exit 1; }
info(){ echo "[INFO] $*"; }
ok()  { echo "[OK] $*"; }

require_docker() {
  command -v docker >/dev/null 2>&1 || die "Docker not found. Install Docker Desktop and retry."
  docker info >/dev/null 2>&1 || die "Docker daemon not running."
}

require_compose() {
  docker compose version >/dev/null 2>&1 || die "docker compose v2 not found. Update Docker Desktop."
}

compose_up_services() {
  [ -f "docker-compose.yml" ] || die "docker-compose.yml not found in $(pwd)"
  info "Starting ${MINIO_SERVICE} + ${DB2_SERVICE} via docker compose…"
  docker compose up -d "${MINIO_SERVICE}" "${DB2_SERVICE}"
}

cid_for() {
  docker compose ps -q "$1"
}

# -----------------------------
# Readiness checks
# -----------------------------
wait_for_minio() {
  info "Waiting for MinIO health endpoint at ${MINIO_ENDPOINT}…"
  local i=0
  until curl -fsS "${MINIO_ENDPOINT}/minio/health/ready" >/dev/null; do
    i=$((i+1))
    if (( i > 60 )); then
      die "MinIO did not report healthy after $i checks."
    fi
    echo "[INFO] …MinIO starting (${i})"
    sleep 2
  done
  ok "MinIO ready."
}

wait_for_port() {
  local host="${1}" port="${2}" label="${3:-port ${2}}"
  info "Waiting for ${label} on ${host}:${port}…"
  local i=0
  while ! (echo >/dev/tcp/${host}/${port}) >/dev/null 2>&1; do
    i=$((i+1))
    if (( i > 120 )); then
      die "${label} did not open."
    fi
    echo "[INFO] …${label} not open yet (${i})"
    sleep 2
  done
  ok "${label} open."
}

# Db2 is notorious for long cold-starts; we add multiple, escalating probes.
# --- waits until Db2 can run an actual SQL query as db2inst1 ---
wait_for_db2_sql() {
  local cid="$1" inst="${DB2_INSTANCE:-db2inst1}" db="${DB2_DBNAME:-TESTDB}"
  local timeout="${DB2_WAIT_SECS:-1200}"   # 20 min for first-boot initialization
  local start_ts now elapsed

  start_ts="$(date +%s)"
  echo "[INFO] Waiting for Db2 SQL connectivity as ${inst} to database ${db}… (timeout=${timeout}s)"

  # Helper used inside the container: source either profile path, then do connect + select 1
  # - tries both ~inst/sqllib and /database/config/inst/sqllib (some variants lay it out there)
  # - starts the instance if needed; creates DB if missing
  local inner_sql_probe='
    set -e
    inst="${DB2_INSTANCE:-db2inst1}"
    db="${DB2_DBNAME:-TESTDB}"

    # Be tolerant if outer shell is `set -u`
    set +u

    prof1="/home/${inst}/sqllib/db2profile"
    prof2="/database/config/${inst}/sqllib/db2profile"
    [ -f "$prof1" ] && source "$prof1" || true
    [ -f "$prof2" ] && source "$prof2" || true

    # Ensure engine is up
    db2start >/dev/null 2>&1 || true

    # Ensure database exists
    db2 connect to "${db}" >/dev/null 2>&1 || db2 "create database ${db}"

    # Round-trip: connect + trivial query
    db2 connect to "${db}" >/dev/null
    db2 "select 1 from sysibm.sysdummy1" >/dev/null
    db2 connect reset >/dev/null
  '

  while true; do
    now="$(date +%s)"; elapsed=$(( now - start_ts ))
    if (( elapsed > timeout )); then
      echo "[WARN] Exceeded ${timeout}s waiting for Db2 SQL connectivity. Recent logs (filtered):"
      docker logs --tail=300 "$cid" 2>&1 | grep -Ev 'RdRnd hardware RNG capability not available' || true
      return 1
    fi

    # First, quick engine/port sanity (non-fatal if flaky)
    if ! docker exec "$cid" bash -lc 'ps -ef | grep -q "[d]b2sysc"'; then
      echo "[INFO] …Db2 engine not fully up yet (${elapsed}s)"
      sleep 5
      continue
    fi

    # Now the real SQL probe as the instance user
    if docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc '$inner_sql_probe'"; then
      echo "[OK] Db2 is SQL-ready (connect + SELECT 1 succeeded)."
      return 0
    fi

    # Periodic, filtered log peek every ~30s so RNG spam doesn’t block visibility
    if (( elapsed % 30 == 0 )); then
      echo "[INFO] …Db2 initializing (${elapsed}s). Recent log (filtered):"
      docker logs --tail=50 "$cid" 2>&1 | grep -Ev 'RdRnd hardware RNG capability not available' || true
    else
      echo "[INFO] …Db2 not SQL-ready yet (${elapsed}s)"
    fi
    sleep 5
  done
}


ensure_db2_database() {
  local cid; cid="$(cid_for "$DB2_SERVICE")"
  [ -n "$cid" ] || die "Db2 container not found for service '$DB2_SERVICE'."

  info "Ensuring database '${DB2_DBNAME}' exists…"
  # We keep the outer shell -e, but make the inner exec idempotent and tolerant
  docker exec "$cid" bash -lc "
    set -euo pipefail
    inst='${DB2_INSTANCE}'
    db='${DB2_DBNAME}'
    su - \"\$inst\" -s /bin/bash -lc '
      set -e
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2start >/dev/null 2>&1 || true

      # If alias already cataloged, skip creation
      if db2 list database directory | grep -iq \"Database alias\\s\\+:\\s*${DB2_DBNAME}\"; then
        echo \"[INFO] Db2 alias \${db} already cataloged — skipping create.\"
      else
        echo \"[INFO] Creating database \${db}…\"
        # Create (ignore SQL1005N alias-exists if a race happened)
        if ! db2 \"create database \${db}\" >/dev/null 2>&1; then
          # If the failure was alias-exists, treat as success
          if db2 list database directory | grep -iq \"Database alias\\s\\+:\\s*${DB2_DBNAME}\"; then
            echo \"[WARN] Create returned non-zero, but alias now exists — continuing.\"
          else
            echo \"[ERROR] Create database \${db} failed and alias not found.\"
            exit 2
          fi
        fi
        # Try to activate (safe if already active)
        db2 \"activate database \${db}\" >/dev/null 2>&1 || true
      fi

      # Final connectivity check — must succeed
      if db2 connect to \${db} >/dev/null 2>&1; then
        db2 \"values 1\" >/dev/null
        db2 connect reset >/dev/null
        echo \"[OK] Db2 database \${db} is available.\"
      else
        echo \"[ERROR] Could not connect to \${db} after ensure.\"
        exit 3
      fi
    '
  "
  ok "Db2 database '${DB2_DBNAME}' is available."
}

# -----------------------------
# Main
# -----------------------------
require_docker
require_compose
compose_up_services

wait_for_minio
wait_for_port 127.0.0.1 "${DB2_PORT_HOST}" "Db2 host port"

cid="$(docker compose ps -q "${DB2_SERVICE:-db2}")"
: "${DB2_INSTANCE:=db2inst1}"
: "${DB2_DBNAME:=TESTDB}"
: "${DB2_WAIT_SECS:=1200}"

wait_for_db2_sql "$cid" || exit 1
echo "[OK] Infrastructure ready: MinIO + Db2"

ensure_db2_database

info "MinIO Console → ${MINIO_CONSOLE_URL}  (user: minioadmin)"
ok   "Infrastructure ready."
