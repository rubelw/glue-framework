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

DB2_CONTAINER_NAME    ?= db2-int
DB2_IMAGE             ?= icr.io/db2_community/db2
DB2_PORT              ?= 50000
DB2_INSTANCE          ?= db2inst1
DB2_PASSWORD          ?= password
DB2_DBNAME            ?= TESTDB
DB2_SAMPLEDB          ?= true

# --- Integration DB knobs (local Postgres just for tests) ---
PG_CONTAINER_NAME ?= pg-int
PG_IMAGE          ?= postgres:15
PG_PORT           ?= 5432
PG_USER           ?= postgres
PG_PASSWORD       ?= password
PG_DB             ?= postgres


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


# --- Shared bridge network for tests ---
ETL_NET ?= etl-int

.PHONY: up-net down-net
up-net:
	@docker network inspect $(ETL_NET) >/dev/null 2>&1 || { \
	  echo "[INFO] Creating network $(ETL_NET)"; \
	  docker network create $(ETL_NET) >/dev/null; \
	}

down-net:
	@docker network rm $(ETL_NET) >/dev/null 2>&1 || true


.PHONY: start stop status



stop:
	@echo "[INFO] Stopping infrastructure…"
	-docker rm -f pg-int db2-int 2>/dev/null || true
	-docker network rm etl-int 2>/dev/null || true
	-docker volume rm glue-framework_db2data glue-framework_pgdata 2>/dev/null || true
	@echo "[OK] Infrastructure stopped and cleaned up."

status:
	@echo "---- Containers ----"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "$(MINIO_CONTAINER_NAME)|$(DB2_CONTAINER_NAME)|NAMES" || echo "(none)"

# Where optional seed templates live (override if you want)
SEED_TEMPLATES ?= seeds

.PHONY: run run-customers-dev seed-dev seed-%-dev ensure_docker pull clean-out test \
        setup-profile-local-s3 check-local-s3 ensure-warehouse-bucket

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


# ---------- Postgres ----------
.PHONY: up-pg down-pg wait-pg

PG_CONTAINER_NAME ?= pg-int
PG_IMAGE          ?= postgres:15
PG_PORT           ?= 5432
PG_USER           ?= postgres
PG_PASSWORD       ?= postgres
PG_DB             ?= postgres

up-pg: up-net
	@echo "[INFO] Starting Postgres '$(PG_CONTAINER_NAME)' on $(ETL_NET) and mapping host port $(PG_PORT)..."
	@docker rm -f $(PG_CONTAINER_NAME) >/dev/null 2>&1 || true
	@docker run -d --name $(PG_CONTAINER_NAME) \
		--network $(ETL_NET) \
		-p $(PG_PORT):5432 \
		-e POSTGRES_USER=$(PG_USER) \
		-e POSTGRES_PASSWORD=$(PG_PASSWORD) \
		-e POSTGRES_DB=$(PG_DB) \
		$(PG_IMAGE) >/dev/null
	@$(MAKE) --no-print-directory wait-pg

down-pg:
	@echo "[INFO] Stopping Postgres test container..."
	@docker rm -f $(PG_CONTAINER_NAME) >/dev/null 2>&1 || true

wait-pg:
	@echo "[INFO] Waiting for Postgres readiness on localhost:$(PG_PORT)..."
	@i=0; \
	while ! (echo >/dev/tcp/127.0.0.1/$(PG_PORT)) >/dev/null 2>&1; do \
	  i=$$((i+1)); [ $$i -gt 60 ] && echo "[ERROR] PG not up after 60s" && exit 1; \
	  sleep 1; \
	done; \
	echo "[INFO] Waiting for pg_isready inside container..."; \
	until docker exec $(PG_CONTAINER_NAME) pg_isready -U $(PG_USER) >/dev/null 2>&1; do \
	  sleep 1; \
	done; \
	echo "[OK] Postgres is ready."

create-pg-db:
	@echo "[INFO] Ensuring database '$(PG_DB)' exists..."
	@docker exec -i $(PG_CONTAINER_NAME) \
	  psql -h 127.0.0.1 -U $(PG_USER) -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$(PG_DB)'" | grep -q 1 || \
	  docker exec -i $(PG_CONTAINER_NAME) \
	    psql -h 127.0.0.1 -U $(PG_USER) -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $(PG_DB)" || true
	@echo "[OK] Database '$(PG_DB)' is present."

# ---------- Db2 (on shared network) ----------
.PHONY: up-db2 down-db2 wait-db2-tcp wait-db2-setup wait-db2-sql seed-db2


up-db2: up-net
	@echo "[INFO] Starting Db2 '$(DB2_CONTAINER_NAME)' on $(ETL_NET) and mapping host port $(DB2_PORT)..."
	@docker rm -f $(DB2_CONTAINER_NAME) >/dev/null 2>&1 || true
	@docker run -d --name $(DB2_CONTAINER_NAME) \
		--network $(ETL_NET) \
		--privileged \
		-e LICENSE=accept \
		-e DB2INSTANCE=$(DB2_INSTANCE) \
		-e DB2INST1_PASSWORD=$(DB2_PASSWORD) \
		-e DBNAME=$(DB2_DBNAME) \
		-p $(DB2_PORT):50000 \
		$(DB2_IMAGE) >/dev/null
	@echo "[INFO] Db2 mapped host:localhost:$(DB2_PORT) -> container:50000"
	@$(MAKE) --no-print-directory wait-db2-tcp wait-db2-setup wait-db2-sql

down-db2:
	@echo "[INFO] Stopping Db2 test container..."
	@docker rm -f $(DB2_CONTAINER_NAME) >/dev/null 2>&1 || true

wait-db2-tcp:
	@echo "[INFO] Waiting for Db2 TCP on localhost:$(DB2_PORT)..."
	@i=0; \
	while ! (echo >/dev/tcp/127.0.0.1/$(DB2_PORT)) >/dev/null 2>&1; do \
	  i=$$((i+1)); [ $$i -gt 180 ] && echo "[ERROR] Db2 TCP not up after 180s" && exit 1; \
	  sleep 2; \
	done; \
	echo "[OK] Db2 TCP is accepting connections."

wait-db2-setup:
	@echo "[INFO] Waiting for Db2 container setup to complete (watching logs for 'Setup has completed')..."
	@i=0; \
	while ! docker logs $(DB2_CONTAINER_NAME) 2>&1 | grep -q "Setup has completed"; do \
	  i=$$((i+1)); [ $$i -gt 450 ] && echo "[ERROR] Db2 setup did not complete after 900s" && exit 1; \
	  sleep 2; \
	done; \
	echo "[OK] Db2 setup has completed."

# IMPORTANT: run as db2inst1 and source profile before any db2 commands.
wait-db2-sql:
	@echo "[INFO] Waiting for Db2 SQL connect to $(DB2_DBNAME) as $(DB2_INSTANCE)…"
	@i=0; \
	while true; do \
	  if docker exec -u $(DB2_INSTANCE) $(DB2_CONTAINER_NAME) bash -lc 'source $$HOME/sqllib/db2profile >/dev/null 2>&1 || true; db2start >/dev/null 2>&1 || true; db2 list db directory' >/dev/null 2>&1; then \
	    break; \
	  fi; \
	  i=$$((i+1)); [ $$i -gt 120 ] && echo "[ERROR] Db2 instance not ready after 240s" && exit 1; \
	  sleep 2; \
	done; \
	# Ensure TESTDB exists (first boot may not have it)
	if ! docker exec -u $(DB2_INSTANCE) $(DB2_CONTAINER_NAME) bash -lc 'source $$HOME/sqllib/db2profile; db2 list db directory | grep -q "$(DB2_DBNAME)"'; then \
	  echo "[INFO] Creating database $(DB2_DBNAME)…"; \
	  docker exec -u $(DB2_INSTANCE) $(DB2_CONTAINER_NAME) bash -lc 'source $$HOME/sqllib/db2profile; db2 -v create database $(DB2_DBNAME) using codeset UTF-8 territory US' >/dev/null; \
	fi; \
	# Final connect check with credentials
	if docker exec -u $(DB2_INSTANCE) $(DB2_CONTAINER_NAME) bash -lc 'source $$HOME/sqllib/db2profile; db2 -v connect to $(DB2_DBNAME) user $(DB2_INSTANCE) using $(DB2_PASSWORD) ; db2 terminate' >/dev/null 2>&1; then \
	  echo "[OK] Db2 SQL is ready."; \
	else \
	  echo "[ERROR] Db2 SQL connect still failing."; exit 1; \
	fi

seed-db2:
	@set -e
	@echo "[INFO] Seeding Db2 TESTDB.DB2INST1.CUSTOMERS_DEV (idempotent)…"
	@docker exec -u db2inst1 db2-int bash -lc 'set -e; source $$HOME/sqllib/db2profile; \
	  db2 -v connect to TESTDB >/dev/null; \
	  if db2 -x "select 1 from syscat.tables where tabschema='\''DB2INST1'\'' and tabname='\''CUSTOMERS_DEV'\''" | grep -q 1; then \
	    echo "[OK] Table already exists."; \
	  else \
	    echo "[INFO] Creating and seeding table…"; \
	    printf "%s\n" \
"CREATE TABLE DB2INST1.CUSTOMERS_DEV (" \
"  CUSTOMER_ID INT NOT NULL PRIMARY KEY," \
"  EMAIL       VARCHAR(255)," \
"  INGEST_DT   DATE" \
");" \
"INSERT INTO DB2INST1.CUSTOMERS_DEV (CUSTOMER_ID, EMAIL, INGEST_DT) VALUES (1, '\''alice@example.com'\'', CURRENT DATE);" \
"INSERT INTO DB2INST1.CUSTOMERS_DEV (CUSTOMER_ID, EMAIL, INGEST_DT) VALUES (2, '\''bob@example.com'\'',   CURRENT DATE);" \
	    | db2 -tvf /dev/stdin ; \
	  fi; \
	  db2 terminate >/dev/null'

# ---------- Run integration tests on the same network ----------
test-integration: ensure_docker up-net up-pg up-db2 seed-db2
	@echo "[INFO] Running integration tests in Glue 5.0 container"
	@docker run --rm -it \
	  -v $$HOME/.aws:/home/hadoop/.aws:ro \
	  -v $$(PWD):/ws \
	  -w /ws \
	  -e AWS_PROFILE="$(AWS_PROFILE)" \
	  -e PYTHONPATH="/ws" \
	  -e RUN_INTEGRATION=1 \
	  -e RUN_DB2_TESTS=1 \
	  -e RUN_PG_TESTS=1 \
	  -e DB2_HOST=host.docker.internal \
	  -e DB2_PORT=$(DB2_PORT) \
	  -e DB2_DBNAME=$(DB2_DBNAME) \
	  -e DB2_USER=$(DB2_INSTANCE) \
	  -e DB2_PASSWORD=$(DB2_PASSWORD) \
	  -e PG_HOST=host.docker.internal \
	  -e PG_PORT=$(PG_PORT) \
	  -e PG_DB=$(PG_DB) \
	  -e PG_USER=$(PG_USER) \
	  -e PG_PASSWORD=$(PG_PASSWORD) \
	  --entrypoint /bin/bash \
	  $(DOCKER_IMAGE) \
	  -lc "python3 -m pip install -U pip pytest && echo '[TEST] Using JDBC jars: /ws/jars/db2jcc4.jar,/ws/jars/postgresql-42.7.4.jar' && python3 -m pytest -m integration --with-integration -vv -ra --durations=10 tests/integration"


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

