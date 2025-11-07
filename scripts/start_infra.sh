#!/usr/bin/env bash
# scripts/start_infra.sh
# Start MinIO, Db2, and Postgres with docker compose and block until they are usable.

set -euo pipefail

# -----------------------------
# Tunables (override via env)
# -----------------------------
MINIO_SERVICE="${MINIO_SERVICE:-minio}"
DB2_SERVICE="${DB2_SERVICE:-db2}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_CONSOLE_URL="${MINIO_CONSOLE_URL:-http://localhost:9001}"

DB2_PORT_HOST="${DB2_PORT_HOST:-50000}"
DB2_INSTANCE="${DB2_INSTANCE:-db2inst1}"
DB2_DBNAME="${DB2_DBNAME:-TESTDB}"
DB2_WAIT_SECS="${DB2_WAIT_SECS:-900}"

POSTGRES_PORT_HOST="${POSTGRES_PORT_HOST:-5432}"
POSTGRES_DB="${POSTGRES_DB:-testdb}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-password}"

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
  info "Starting ${MINIO_SERVICE}, ${DB2_SERVICE}, and ${POSTGRES_SERVICE} via docker compose…"
  docker compose up -d "${MINIO_SERVICE}" "${DB2_SERVICE}" "${POSTGRES_SERVICE}"
}

cid_for() {
  docker compose ps -q "$1"
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

wait_for_postgres() {
  local cid; cid="$(cid_for "$POSTGRES_SERVICE")"
  [ -n "$cid" ] || die "Postgres container not found."
  info "Waiting for PostgreSQL readiness (port ${POSTGRES_PORT_HOST})…"
  local i=0
  until docker exec "$cid" pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; do
    i=$((i+1))
    if (( i > 60 )); then
      docker logs "$cid" | tail -n 50
      die "PostgreSQL did not become ready after $i checks."
    fi
    echo "[INFO] …Postgres initializing (${i})"
    sleep 2
  done
  ok "PostgreSQL ready at localhost:${POSTGRES_PORT_HOST}/${POSTGRES_DB}"
}

# -----------------------------
# Db2 helpers (inline)
# -----------------------------

# -----------------------------
# Db2 helpers (robust)
# -----------------------------

# Phase A: Wait for the instance CLI to respond (no database connection yet)
wait_for_db2_instance() {
  local cid="$1"
  local inst="${DB2_INSTANCE:-db2inst1}"
  local timeout="${DB2_WAIT_SECS:-900}"

  info "Waiting for Db2 instance CLI in '${cid}' (instance ${inst})…"
  local start_ts now elapsed
  start_ts="$(date +%s)"

  while true; do
    # Anything that doesn't require a DB connection is fine; 'db2level' is reliable.
    if docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc 'source ~/sqllib/db2profile >/dev/null 2>&1 || true; db2level >/dev/null 2>&1'"; then
      ok "Db2 instance CLI responds."
      return 0
    fi
    now="$(date +%s)"; elapsed=$(( now - start_ts ))
    if (( elapsed > timeout )); then
      docker logs "$cid" | tail -n 100 || true
      die "Db2 instance did not become ready within ${timeout}s."
    fi
    echo "[INFO] …Db2 instance not ready yet (${elapsed}s)"
    sleep 5
  done
}

# Ensure DB exists (idempotent)
ensure_db2_database() {
  local cid="$1"
  local inst="${DB2_INSTANCE:-db2inst1}"
  local db="${DB2_DBNAME:-TESTDB}"

  info "Ensuring Db2 database '${db}' exists…"
  # If DB already cataloged, skip creation.
  if docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc 'source ~/sqllib/db2profile >/dev/null 2>&1 || true; db2 list db directory | grep -qi \"Database name.*${db}\"'"; then
    ok "Db2 database '${db}' already exists."
    return 0
  fi

  info "Creating Db2 database '${db}' (AUTOMATIC STORAGE, UTF-8, 32K pages)…"
  docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc '
    set -e
    source ~/sqllib/db2profile >/dev/null 2>&1 || true
    db2 create database ${db} automatic storage yes using codeset UTF-8 territory US collate using system pagesize 32768
  '"
  ok "Created Db2 database '${db}'."
}

# Phase B: Wait until we can CONNECT to the DB and run a trivial SQL
wait_for_db2_connect() {
  local cid="$1"
  local inst="${DB2_INSTANCE:-db2inst1}"
  local db="${DB2_DBNAME:-TESTDB}"
  local timeout="${DB2_WAIT_SECS:-900}"

  info "Waiting for Db2 DB connection to '${db}'…"
  local start_ts now elapsed
  start_ts="$(date +%s)"

  while true; do
    if docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc '
      source ~/sqllib/db2profile >/dev/null 2>&1 || true
      db2 connect to ${db} >/dev/null 2>&1 || exit 1
      db2 -x \"values 1\" >/dev/null 2>&1 || { db2 connect reset >/dev/null 2>&1; exit 2; }
      db2 connect reset >/dev/null 2>&1
    '"; then
      ok "Db2 DB '${db}' accepts SQL."
      return 0
    fi
    now="$(date +%s)"; elapsed=$(( now - start_ts ))
    if (( elapsed > timeout )); then
      docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc 'source ~/sqllib/db2profile >/dev/null 2>&1 || true; db2diag -A >/dev/null 2>&1 || true; tail -n 200 ~/sqllib/db2dump/db2diag.log || true'" || true
      die "Db2 DB '${db}' did not become connectable within ${timeout}s."
    fi
    echo "[INFO] …Db2 DB not connectable yet (${elapsed}s)"
    sleep 5
  done
}

# Create DB if missing (idempotent)
ensure_db2_database() {
  local cid="$1"
  local inst="${DB2_INSTANCE:-db2inst1}"
  local db="${DB2_DBNAME:-TESTDB}"

  info "Ensuring Db2 database '${db}' exists…"
  # If the DB is already cataloged, we are done
  if docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc 'source ~/sqllib/db2profile >/dev/null 2>&1 || true; db2 list db directory | grep -qi \"Database name.*${db}\"'"; then
    ok "Db2 database '${db}' already exists."
    return 0
  fi

  info "Creating Db2 database '${db}' (AUTOMATIC STORAGE, PAGESIZE 32K)…"
  docker exec "$cid" bash -lc "su - ${inst} -s /bin/bash -lc '
    set -e
    source ~/sqllib/db2profile >/dev/null 2>&1 || true
    db2 create database ${db} automatic storage yes using codeset UTF-8 territory US collate using system pagesize 32768
  '"
  ok "Created Db2 database '${db}'."
}

# -----------------------------
# Main
# -----------------------------
require_docker
require_compose
compose_up_services

wait_for_minio
wait_for_port 127.0.0.1 "${DB2_PORT_HOST}" "Db2 host port"
wait_for_port 127.0.0.1 "${POSTGRES_PORT_HOST}" "Postgres host port"

cid_db2="$(cid_for "${DB2_SERVICE}")"
[ -n "${cid_db2}" ] || die "Db2 container not found."

# Db2 readiness sequence
wait_for_db2_instance "${cid_db2}"
ensure_db2_database   "${cid_db2}"
wait_for_db2_connect  "${cid_db2}"

# PostgreSQL readiness
wait_for_postgres

ok "Infrastructure ready: MinIO + Db2 + PostgreSQL"
info "MinIO Console → ${MINIO_CONSOLE_URL}  (user: minioadmin)"
info "PostgreSQL → postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT_HOST}/${POSTGRES_DB}"
