#!/usr/bin/env bash
set -euo pipefail

JOB="${1:-}"
ENV_NAME="${2:-${ENV:-dev}}"
shift 2 || true

if [[ -z "$JOB" ]]; then
  echo "Usage: scripts/run_job.sh <job_name> <env> [-- EXTRA_SPARK_ARGS...]"
  exit 1
fi

# Verify Docker is up (macOS convenience)
if ! command -v docker &>/dev/null; then
  echo "[ERROR] Docker not found. Install Docker Desktop and ensure it is running."
  exit 1
fi
if ! docker info &>/dev/null; then
  echo "[INFO] Starting Docker Desktop (macOS)..."
  if [[ "$(uname -s)" == "Darwin" ]]; then open -a Docker || true; fi
  ATTEMPTS=0
  until docker info &>/dev/null || [[ $ATTEMPTS -gt 20 ]]; do
    sleep 2; ((ATTEMPTS++)); echo "Waiting for Docker daemon... ($ATTEMPTS)"
  done
fi

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB_DIR="${WORKDIR}/jobs/${JOB}"
JOB_MAIN="${JOB_DIR}/main.py"
CFG_FILE="${JOB_DIR}/config/${ENV_NAME}.json"
REQ_FILE="${JOB_DIR}/requirements.txt"

[[ -f "$JOB_MAIN" ]] || { echo "[ERROR] Job main not found: $JOB_MAIN"; exit 1; }
[[ -f "$CFG_FILE" ]] || { echo "[ERROR] Config not found: $CFG_FILE"; exit 1; }

# Extra Python deps (pure-Python only) installed into a venv inside the container
PY_DEPS_CMD=""
if [[ -f "$REQ_FILE" ]]; then
  PY_DEPS_CMD="python3.11 -m venv /tmp/venv && \
               . /tmp/venv/bin/activate && \
               pip install -U pip && \
               pip install -r /ws/jobs/${JOB}/requirements.txt && \
               export PYSPARK_PYTHON=/tmp/venv/bin/python && \
               export PYSPARK_DRIVER_PYTHON=/tmp/venv/bin/python"
fi

# Spark/Glue defaults for performance and observability
SPARK_CONF=(
  "--conf" "spark.sql.adaptive.enabled=true"
  "--conf" "spark.sql.shuffle.partitions=auto"
  "--conf" "spark.ui.showConsoleProgress=true"
)

# Pass canonical Glue-style args. Locally we read a *file* config,
# but you can still simulate S3 by using s3a:// and enabling creds if desired.
GLUE_ARGS=(
  "--ENV=${ENV_NAME}"
  "--CONFIG_S3_URI=file:///ws/jobs/${JOB}/config/${ENV_NAME}.json"
  "--BOOKMARKED=false"
  "--enable-glue-datacatalog=true"
  "--enable-metrics=true"
  "--enable-job-insights=true"
)

# Compose service: choose glue-framework or glue2 to avoid UI port conflict for parallel runs
SERVICE="${SERVICE:-glue-framework}"

echo "[INFO] Running job='${JOB}' env='${ENV_NAME}' via service='${SERVICE}'"

docker compose run --rm \
  -e AWS_PROFILE \
  -e ENV="${ENV_NAME}" \
  "$SERVICE" \
  bash -lc "
    set -euo pipefail
    $PY_DEPS_CMD
    echo '[INFO] Spark submitting ${JOB} (${ENV_NAME})...'
    spark-submit \
      ${SPARK_CONF[*]} \
      /ws/jobs/${JOB}/main.py \
      ${GLUE_ARGS[*]} \
      $*
  "
