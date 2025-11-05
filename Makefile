# Makefile
SHELL := /bin/bash

# ---- Configurable knobs ----
AWS_PROFILE   ?= default
JOB           ?= customers_etl
ENV           ?= dev
SPARK_UI_PORT ?= 4040
HISTORY_PORT  ?= 18080
DOCKER_IMAGE  ?= public.ecr.aws/glue/aws-glue-libs:5

# Optional: pass extra Spark conf from CLI like:
#   make run JOB=foo ENV=dev EXTRA_CONF='--conf x=y'
EXTRA_CONF    ?=

# Use bash and sane flags
.SHELLFLAGS := -eu -o pipefail -c

# Where optional seed templates live (override if you want)
SEED_TEMPLATES ?= seeds

.PHONY: run run-customers-dev seed-dev seed-%-dev ensure_docker pull clean-out test-fast test

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
	@cfg="jobs/$(JOB)/config/$(ENV).json"; \
	  if [ ! -f "$$cfg" ]; then \
	    echo "[ERROR] Missing config: $$cfg"; exit 1; \
	  fi; \
	  if ! command -v python3 >/dev/null 2>&1; then \
	    echo "[ERROR] python3 is required on the host to parse config JSON."; exit 1; \
	  fi; \
	  fmt=$$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); e=sys.argv[2]; print(d.get(e,{}).get("sink",{}).get("format","parquet"))' "$$cfg" "$(ENV)"); \
	  catalog=$$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); e=sys.argv[2]; print(d.get(e,{}).get("sink",{}).get("catalog",""))' "$$cfg" "$(ENV)"); \
	  ec_extra="$(EXTRA_CONF)"; \
	  if [ "$$fmt" = "iceberg" ]; then \
	    if [ "$$catalog" = "local" ] || [ "$$catalog" = "hadoop" ] || [ -z "$$catalog" ]; then \
	      echo "[INFO] Detected Iceberg job with HadoopCatalog (local). Injecting Spark Iceberg conf..."; \
	      mkdir -p warehouse; \
	      ec_extra="$$ec_extra \
	        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	        --conf spark.sql.catalog.local.type=hadoop \
	        --conf spark.sql.catalog.local.warehouse=file:///ws/warehouse"; \
	    else \
	      echo "[INFO] Iceberg detected with non-local catalog '$$catalog' — leaving conf to your environment (use EXTRA_CONF if needed)."; \
	    fi; \
	  fi; \
	  cmd="spark-submit \
	        $$ec_extra \
	        --conf spark.sql.adaptive.enabled=true \
	        --conf spark.sql.shuffle.partitions=64 \
	        /ws/jobs/$(JOB)/main.py \
	        --ENV=$(ENV) \
	        --CONFIG_S3_URI=file:///ws/jobs/$(JOB)/config/$(ENV).json \
	        --BOOKMARKED=false"; \
	  echo "[INFO] spark-submit command:"; echo "$$cmd"; echo ""; \
	  docker run --rm -it \
	    -v $$HOME/.aws:/home/hadoop/.aws:ro \
	    -v $$(PWD):/ws \
	    -w /ws \
	    -e AWS_PROFILE="$(AWS_PROFILE)" \
	    -e PYTHONPATH="/ws" \
	    -p $(SPARK_UI_PORT):4040 -p $(HISTORY_PORT):18080 \
	    --entrypoint /bin/bash \
	    $(DOCKER_IMAGE) \
	    -lc "$$cmd"

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
