#!/usr/bin/env bash
# scripts/stop_infra.sh
# Stop MinIO and Db2, remove volumes (including db2data), and clean up.

set -euo pipefail

MINIO_CONTAINER_NAME="${MINIO_CONTAINER_NAME:-minio}"
DB2_CONTAINER_NAME="${DB2_CONTAINER_NAME:-db2}"
DB2_VOLUME_NAME="${DB2_VOLUME_NAME:-glue-framework_db2data}"

echo "[INFO] Stopping infrastructure…"

# Detect docker + compose
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 \
   && [ -f docker-compose.yml ] && grep -qE '^[[:space:]]*services:[[:space:]]*$' docker-compose.yml; then
  echo "[INFO] Stopping DB2 + MinIO via docker compose…"
  docker compose down -v || {
    echo "[WARN] docker compose down failed, trying stop + rm…"
    docker compose stop db2 minio || true
    docker compose rm -fsv db2 minio || true
  }
else
  echo "[INFO] Stopping raw containers (if running)…"
  if docker ps --format '{{.Names}}' | grep -q "^${DB2_CONTAINER_NAME}$"; then
    docker stop "${DB2_CONTAINER_NAME}" >/dev/null && echo "[OK] DB2 stopped."
    docker rm -f "${DB2_CONTAINER_NAME}" >/dev/null && echo "[OK] DB2 removed."
  else
    echo "[INFO] DB2 not running."
  fi

  if docker ps --format '{{.Names}}' | grep -q "^${MINIO_CONTAINER_NAME}$"; then
    docker stop "${MINIO_CONTAINER_NAME}" >/dev/null && echo "[OK] MinIO stopped."
    docker rm -f "${MINIO_CONTAINER_NAME}" >/dev/null && echo "[OK] MinIO removed."
  else
    echo "[INFO] MinIO not running."
  fi
fi

# Clean up persistent volume (DB2 data)
if docker volume ls --format '{{.Name}}' | grep -q "^${DB2_VOLUME_NAME}$"; then
  echo "[INFO] Removing volume '${DB2_VOLUME_NAME}'…"
  docker volume rm -f "${DB2_VOLUME_NAME}" >/dev/null && echo "[OK] Volume '${DB2_VOLUME_NAME}' removed."
else
  echo "[INFO] No volume named '${DB2_VOLUME_NAME}' found."
fi

echo "[OK] Infrastructure stopped and cleaned up."
