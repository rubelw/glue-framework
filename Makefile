# Makefile
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# ---- Configurable knobs ----
AWS_PROFILE   ?= default
JOB           ?= customers_etl
ENV           ?= dev
SPARK_UI_PORT ?= 4040
HISTORY_PORT  ?= 18080
DOCKER_IMAGE  ?= public.ecr.aws/glue/aws-glue-libs:5
EXTRA_CONF    ?=
IS_ICEBERG_JOB := $(shell grep -q '"format": "iceberg"' jobs/$(JOB)/config/$(ENV).json && echo 1 || echo 0)


# ---- Local S3 / DB2 knobs ----
LOCAL_S3_PROFILE      ?= local-s3
LOCAL_S3_ACCESS_KEY   ?= minioadmin
LOCAL_S3_SECRET_KEY   ?= minioadmin
LOCAL_S3_REGION       ?= us-east-1
MINIO_ENDPOINT        ?= http://localhost:9000
MINIO_CONTAINER_NAME  ?= minio
MINIO_IMAGE           ?= quay.io/minio/minio:latest

# DB2 runs x86_64; force emulation on Apple Silicon
DB2_PLATFORM ?= linux/amd64

DB2_CONTAINER_NAME    ?= db2
DB2_IMAGE             ?= icr.io/db2_community/db2
DB2_PORT              ?= 50000
DB2_INSTANCE          ?= db2inst1
DB2_PASSWORD          ?= password
DB2_DBNAME            ?= TESTDB
DB2_SAMPLEDB          ?= true

# --- Step 3: Auto-inject Iceberg config when config JSON says "format": "iceberg"
# Detect if this job/env is Iceberg based on the config JSON
IS_ICEBERG_JOB := $(shell grep -q '"format"[[:space:]]*:[[:space:]]*"iceberg"' jobs/$(JOB)/config/$(ENV).json 2>/dev/null && echo 1 || echo 0)

# Use an endpoint that works from inside the container across macOS/Windows/Linux
# (Docker Desktop maps host.docker.internal -> host where MinIO is listening)
ICEBERG_S3_ENDPOINT ?= http://host.docker.internal:9000

ifeq ($(IS_ICEBERG_JOB),1)
  ICEBERG_CONF = \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=$(ICEBERG_S3_ENDPOINT) \
    --conf spark.hadoop.fs.s3a.access.key=$(LOCAL_S3_ACCESS_KEY) \
    --conf spark.hadoop.fs.s3a.secret.key=$(LOCAL_S3_SECRET_KEY) \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
else
  ICEBERG_CONF =
endif

# Detect docker compose
HAS_COMPOSE := $(shell command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 && echo 1 || echo 0)

.PHONY: start stop status

start: ensure_docker
	@MINIO_CONTAINER_NAME='$(MINIO_CONTAINER_NAME)' \
	 MINIO_IMAGE='$(MINIO_IMAGE)' \
	 LOCAL_S3_ACCESS_KEY='$(LOCAL_S3_ACCESS_KEY)' \
	 LOCAL_S3_SECRET_KEY='$(LOCAL_S3_SECRET_KEY)' \
	 MINIO_ENDPOINT='$(MINIO_ENDPOINT)' \
	 DB2_CONTAINER_NAME='$(DB2_CONTAINER_NAME)' \
	 DB2_IMAGE='$(DB2_IMAGE)' \
	 DB2_PORT='$(DB2_PORT)' \
	 DB2_INSTANCE='$(DB2_INSTANCE)' \
	 DB2_PASSWORD='$(DB2_PASSWORD)' \
	 DB2_DBNAME='$(DB2_DBNAME)' \
	 DB2_SAMPLEDB='$(DB2_SAMPLEDB)' \
	 bash scripts/start_infra.sh

stop:
	@MINIO_CONTAINER_NAME='$(MINIO_CONTAINER_NAME)' \
	 DB2_CONTAINER_NAME='$(DB2_CONTAINER_NAME)' \
	 bash scripts/stop_infra.sh

status:
	@echo "---- Containers ----"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "$(MINIO_CONTAINER_NAME)|$(DB2_CONTAINER_NAME)|NAMES" || echo "(none)"

# Where optional seed templates live (override if you want)
SEED_TEMPLATES ?= seeds

.PHONY: run run-customers-dev seed-dev seed-%-dev ensure_docker pull clean-out test-fast test \
        setup setup-profile-local-s3 check-local-s3 ensure-warehouse-bucket

# Pull the Glue image (optional but nice the first time)
pull:
	docker pull $(DOCKER_IMAGE)

# Ensure Docker Desktop/daemon is up before running
ensure_docker:
	@command -v docker >/dev/null || (echo "[ERROR] Docker not found. Install Docker Desktop and retry." && exit 1)
	@docker info >/dev/null 2>&1 || ( \
		echo "[INFO] Docker daemon not running. Trying to start Docker Desktop (macOS)..."; \
		([ "$$(uname -s)" = "Darwin" ] && open -a Docker || true); \
		i=0; \
		while ! docker info >/dev/null 2>&1 && [ $$i -lt 30 ]; do \
			sleep 2; i=$$((i+1)); echo "[INFO] Waiting for Docker... ($$i)"; \
		done; \
		docker info >/dev/null 2>&1 || (echo "[ERROR] Docker daemon still not ready." && exit 1) \
	)

# ---------------------------------------
# Run the job (auto Iceberg HadoopCatalog)
# ---------------------------------------
run: ensure_docker
	@echo "[INFO] Running job='$(JOB)' env='$(ENV)'"
	docker run --rm -it \
	  -v $$HOME/.aws:/home/hadoop/.aws:ro \
	  -v $$(PWD):/ws \
	  -w /ws \
	  -e AWS_PROFILE="$(AWS_PROFILE)" \
	  -e PYTHONPATH="/ws" \
	  -p $(SPARK_UI_PORT):4040 -p $(HISTORY_PORT):18080 \
	  --entrypoint /bin/bash \
	  $(DOCKER_IMAGE) \
	  -lc 'spark-submit \
	        $(ICEBERG_CONF) \
	        --conf spark.sql.shuffle.partitions=64 \
	        /ws/jobs/$(JOB)/main.py \
	        --ENV=$(ENV) \
	        --CONFIG_S3_URI=file:///ws/jobs/$(JOB)/config/$(ENV).json \
	        --BOOKMARKED=false'

# Convenience alias to run exactly your customers_etl dev command
run-customers-dev: JOB = customers_etl
run-customers-dev: ENV = dev
run-customers-dev: run

# -----------------
# Seeding utilities
# -----------------

# Seed a single dataset's dev folder.
# Usage: make seed-orders-dev   or   make seed-customers-dev
seed-%-dev:
	@dataset=$* ; \
	data_dir="data/$$dataset/dev"; \
	echo "[INFO] Seeding $$dataset for dev environment..."; \
	mkdir -p "$$data_dir"; \
	if ls "$$data_dir"/*.csv >/dev/null 2>&1; then \
	  echo "[OK] Existing CSV files found in $$data_dir — skipping seed."; \
	elif [ -f "$(SEED_TEMPLATES)/$$dataset.csv" ]; then \
	  cp "$(SEED_TEMPLATES)/$$dataset.csv" "$$data_dir/"; \
	  echo "[OK] Copied template from $(SEED_TEMPLATES)/$$dataset.csv"; \
	else \
	  echo "[WARN] No template or existing data found for $$dataset, creating placeholder"; \
	  echo "id,value" > "$$data_dir/sample.csv"; \
	  echo "[OK] Wrote $$data_dir/sample.csv"; \
	fi

# Seed every dataset dir under ./data
seed-dev:
	@echo "[INFO] Seeding all datasets under ./data/"
	@for d in data/*; do \
	  if [ -d "$$d" ]; then \
	    name=$$(basename "$$d"); \
	    $(MAKE) --no-print-directory seed-$$name-dev; \
	  fi; \
	done
	@echo "[DONE] All available datasets seeded."

# Clean outputs
clean-out:
	@rm -rf out/* || true
	@echo "[OK] Cleared ./out"

# -----------
# Test targets
# -----------
test-fast: ensure_docker
	docker run --rm -it \
	  -v $$HOME/.aws:/home/hadoop/.aws:ro \
	  -v $$(PWD):/ws \
	  -w /ws \
	  -e AWS_PROFILE="$(AWS_PROFILE)" \
	  --entrypoint /bin/bash \
	  $(DOCKER_IMAGE) \
	  -lc 'python3 -m pip install -U pip pytest && PYTHONPATH=/ws python3 -m pytest -q tests/unit/test_transform.py'

test: ensure_docker
	@echo "[INFO] Running unit tests in Glue 5.0 container"
	docker run --rm -it \
	  -v $$HOME/.aws:/home/hadoop/.aws:ro \
	  -v $$(PWD):/ws \
	  -w /ws \
	  -e AWS_PROFILE="$(AWS_PROFILE)" \
	  --entrypoint /bin/bash \
	  $(DOCKER_IMAGE) \
	  -lc 'python3 -m pip install -U pip pytest && PYTHONPATH=/ws python3 -m pytest -vv -ra --durations=10 --junitxml=out/test-results.xml'

# -----------------------------------------------------------------------------
# Step 2. Setup local S3 + warehouse (delegates to scripts/setup_infra.sh)
# -----------------------------------------------------------------------------
setup:
	@echo "[INFO] Running setup for local S3 and warehouse bucket..."
	@if [ ! -f scripts/setup_infra.sh ]; then \
		echo "[ERROR] scripts/setup_infra.sh not found. Please make sure it exists."; \
		exit 1; \
	fi
	@chmod +x scripts/setup_infra.sh
	@./scripts/setup_infra.sh