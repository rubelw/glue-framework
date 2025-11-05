# Makefile
SHELL := /bin/bash

# ---- Configurable knobs ----
AWS_PROFILE ?= default
JOB ?= customers_etl
ENV ?= dev
GLUE_IMAGE ?= public.ecr.aws/glue/aws-glue-libs:5
SPARK_UI_PORT ?= 4040
HISTORY_PORT ?= 18080

# Use bash and sane flags
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Where optional seed templates live (override if you want)
SEED_TEMPLATES ?= seeds


# ---- Internal: the exact docker run you asked for (parameterized) ----
RUN_CMD = docker run --rm -it \
  -v $$HOME/.aws:/home/hadoop/.aws:ro \
  -v $$(PWD):/ws \
  -w /ws \
  -e AWS_PROFILE="$(AWS_PROFILE)" \
  -e PYTHONPATH="/ws" \
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

.PHONY: run run-customers-dev seed-dev seed-%-dev ensure_docker pull

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
	  -lc 'python3 -m pip install -U pip pytest && PYTHONPATH=/ws python3 -m pytest -vv -ra --durations=10 --junitxml=out/test-results.xml'