# Makefile
SHELL := /bin/bash

# ---- Configurable knobs ----
AWS_PROFILE ?= default
JOB ?= customers_etl
ENV ?= dev
GLUE_IMAGE ?= public.ecr.aws/glue/aws-glue-libs:5
SPARK_UI_PORT ?= 4040
HISTORY_PORT ?= 18080

# ---- Internal: the exact docker run you asked for (parameterized) ----
RUN_CMD = docker run --rm -it \
  -v $$HOME/.aws:/home/hadoop/.aws:ro \
  -v $$(PWD):/ws \
  -w /ws \
  -e AWS_PROFILE="$(AWS_PROFILE)" \
  -p $(SPARK_UI_PORT):4040 -p $(HISTORY_PORT):18080 \
  --entrypoint /bin/bash \
  $(GLUE_IMAGE) \
  -lc 'spark-submit \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.shuffle.partitions=64 \
        /ws/jobs/$(JOB)/main.py \
        --ENV=$(ENV) \
        --CONFIG_S3_URI=file:///ws/jobs/$(JOB)/config/$(ENV).json \
        --BOOKMARKED=false'

.PHONY: run run-customers-dev seed-dev-data ensure_docker pull

# Run the job (defaults: JOB=customers_etl, ENV=dev)
run: ensure_docker
	@echo "[INFO] Running job='$(JOB)' env='$(ENV)'"
	@$(RUN_CMD)

# Convenience alias to run exactly your customers_etl dev command
run-customers-dev: JOB = customers_etl
run-customers-dev: ENV = dev
run-customers-dev: run

# Pull the Glue image (optional but nice the first time)
pull:
	docker pull $(GLUE_IMAGE)

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

# Seed a tiny CSV dataset so the job has input files
seed-dev-data:
	@mkdir -p data/customers/dev
	@printf "customer_id,email,ingest_dt\n1,Alice@example.com,2025-11-01\n2,bob@Example.com,2025-11-01\n2,bob@Example.com,2025-11-01\n3,Carla@EXAMPLE.COM,2025-11-02\n" > data/customers/dev/customers_part1.csv
	@echo "[OK] Wrote data/customers/dev/customers_part1.csv"

.PHONY: test-fast
test-fast: ensure_docker
	docker run --rm -it \
	  -v $$HOME/.aws:/home/hadoop/.aws:ro \
	  -v $$(PWD):/ws \
	  -w /ws \
	  -e AWS_PROFILE="$(AWS_PROFILE)" \
	  --entrypoint /bin/bash \
	  $(GLUE_IMAGE) \
	  -lc 'python3 -m pip install -U pip pytest && PYTHONPATH=/ws python3 -m pytest -q tests/unit/test_transform.py'

.PHONY: test
test: ensure_docker
	@echo "[INFO] Running unit tests in Glue 5.0 container"
	docker run --rm -it \
	  -v $$HOME/.aws:/home/hadoop/.aws:ro \
	  -v $$(PWD):/ws \
	  -w /ws \
	  -e AWS_PROFILE="$(AWS_PROFILE)" \
	  --entrypoint /bin/bash \
	  $(GLUE_IMAGE) \
	  -lc 'python3 -m pip install -U pip pytest && PYTHONPATH=/ws python3 -m pytest -q'