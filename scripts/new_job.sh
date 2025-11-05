#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# new_job.sh — scaffold a new Glue job using the project's shared lib/
# Usage:
#   scripts/new_job.sh <job_name>
#     [--dataset <dataset>]                   # single-source dataset name (e.g., customers)
#     [--two-source <src1,src2>]              # two-source datasets (e.g., orders,customers)
#     [--partition <col>]                     # partition column (default: ingest_dt)
#     [--join-key <col>]                      # join key (default: customer_id)
#     [--seed]                                # create seeds/<dataset>.csv if missing
#
# Examples:
#   scripts/new_job.sh customers_etl --dataset customers --partition ingest_dt --seed
#   scripts/new_job.sh orders_enrichment --two-source orders,customers --partition order_dt --join-key customer_id --seed
# ------------------------------------------------------------------------------

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

JOB_NAME="${1:-}"; shift || true
[[ -n "${JOB_NAME}" ]] || { echo "[ERROR] Missing <job_name>"; exit 1; }

DATASET=""                 # for single-source
TWO_SOURCE=""              # e.g. "orders,customers"
PARTITION_COL="ingest_dt"
JOIN_KEY="customer_id"
DO_SEED=0

while (( $# )); do
  case "$1" in
    --dataset)    DATASET="${2:-}"; shift 2;;
    --two-source) TWO_SOURCE="${2:-}"; shift 2;;
    --partition)  PARTITION_COL="${2:-}"; shift 2;;
    --join-key)   JOIN_KEY="${2:-}"; shift 2;;
    --seed)       DO_SEED=1; shift;;
    -h|--help)
      grep -E "^# " "$0" | sed 's/^# //'; exit 0;;
    *)
      echo "[ERROR] Unknown arg: $1"; exit 1;;
  esac
done

# Validate flags
if [[ -n "$DATASET" && -n "$TWO_SOURCE" ]]; then
  echo "[ERROR] Use either --dataset OR --two-source, not both."; exit 1
fi
if [[ -z "$DATASET" && -z "$TWO_SOURCE" ]]; then
  echo "[INFO] No dataset flag provided; defaulting to single-source with dataset 'example'."
  DATASET="example"
fi

mkdir -p "$ROOT/jobs/$JOB_NAME/config"
touch "$ROOT/jobs/__init__.py" "$ROOT/jobs/$JOB_NAME/__init__.py" "$ROOT/lib/__init__.py"

# ------------------------------
# Helpers for config scaffolding
# ------------------------------
json_escape() { python3 - <<'PY'
import json,sys
print(json.dumps(sys.stdin.read()))
PY
}

# ------------------------------
# MAIN.PY TEMPLATE (single-source)
# ------------------------------
emit_single_source_main() {
  cat > "$ROOT/jobs/$JOB_NAME/main.py" <<'PY'
import sys
from pyspark.sql import SparkSession, functions as F

from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats

args = parse_args(sys.argv)
spark = SparkSession.builder.getOrCreate()

cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

# Config-driven settings
source_paths = cfg.get("source_paths", {})  # {"primary": "file:///ws/data/<dataset>/dev/*.csv"}
target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "INGEST_DT_PLACEHOLDER")
join_key     = cfg.get("join_key", "JOIN_KEY_PLACEHOLDER")

# Local guards (no-op for s3a://)
for label, path in source_paths.items():
    must_exist_glob(path, label=f"source:{label}", if_scheme="file")
ensure_dir(target_path, scheme="file")

# Read primary source
primary_path = source_paths.get("primary")
df = read_csv(spark, primary_path)

# Required columns: always include partition column; include join key if present
req_cols = []
if partition_by: req_cols.append(partition_by)
if join_key:     req_cols.append(join_key)
require_columns(df, req_cols)

# Typical cleaning
if "email" in df.columns:
    df = normalize_lower(df, "email")
if join_key and join_key in df.columns:
    df = dedupe_by_keys(df, [join_key])

df = df.withColumn("load_ts", F.current_timestamp())
write_parquet(df, target_path, repartition=repartition, partitionBy=partition_by)

emit_run_stats(
    env=env,
    rows_out=df.count(),
    target=target_path,
    job="JOB_NAME_PLACEHOLDER"
)
PY
  # fill placeholders
  python3 - "$ROOT/jobs/$JOB_NAME/main.py" <<PY
import sys,pathlib
p=pathlib.Path(sys.argv[1])
s=p.read_text()
s=s.replace("INGEST_DT_PLACEHOLDER","${PARTITION_COL}")
s=s.replace("JOIN_KEY_PLACEHOLDER","${JOIN_KEY}")
s=s.replace("JOB_NAME_PLACEHOLDER","${JOB_NAME}")
p.write_text(s)
PY
}

# ------------------------------
# MAIN.PY TEMPLATE (two-source join)
# ------------------------------
emit_two_source_main() {
  cat > "$ROOT/jobs/$JOB_NAME/main.py" <<'PY'
import sys
from pyspark.sql import SparkSession, functions as F

from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats

args = parse_args(sys.argv)
spark = SparkSession.builder.getOrCreate()

cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

# Config-driven settings
source_paths = cfg.get("source_paths", {})  # {"left": "...", "right": "..."}
target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "INGEST_DT_PLACEHOLDER")
join_key     = cfg.get("join_key", "JOIN_KEY_PLACEHOLDER")

# Guards
for label, path in source_paths.items():
    must_exist_glob(path, label=f"source:{label}", if_scheme="file")
ensure_dir(target_path, scheme="file")

left_path  = source_paths.get("left")
right_path = source_paths.get("right")

left  = read_csv(spark, left_path)
right = read_csv(spark, right_path)

# Require key + partition in left; key in right
req_left  = [join_key] + ([partition_by] if partition_by else [])
req_right = [join_key]
require_columns(left, req_left)
require_columns(right, req_right)

# Normalize/clean
for df in (left, right):
    if "email" in df.columns:
        df = normalize_lower(df, "email")

right = right.dropDuplicates([join_key])

enriched = (
    left.join(F.broadcast(right), on=join_key, how="left")
         .withColumn("load_ts", F.current_timestamp())
)

write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)

emit_run_stats(
    env=env,
    rows_in_left=left.count(),
    rows_in_right=right.count(),
    rows_out=enriched.count(),
    target=target_path,
    job="JOB_NAME_PLACEHOLDER"
)
PY
  python3 - "$ROOT/jobs/$JOB_NAME/main.py" <<PY
import sys,pathlib
p=pathlib.Path(sys.argv[1])
s=p.read_text()
s=s.replace("INGEST_DT_PLACEHOLDER","${PARTITION_COL}")
s=s.replace("JOIN_KEY_PLACEHOLDER","${JOIN_KEY}")
s=s.replace("JOB_NAME_PLACEHOLDER","${JOB_NAME}")
p.write_text(s)
PY
}

# ------------------------------
# CONFIG templates
# ------------------------------
emit_single_source_configs() {
  local ds="${DATASET:-example}"
  cat > "$ROOT/jobs/$JOB_NAME/config/dev.json" <<JSON
{
  "dev": {
    "source_paths": {
      "primary": "file:///ws/data/${ds}/dev/*.csv"
    },
    "target_path": "file:///ws/out/${JOB_NAME}/dev/",
    "repartition": 4,
    "partition_col": "${PARTITION_COL}",
    "join_key": "${JOIN_KEY}"
  }
}
JSON

  cat > "$ROOT/jobs/$JOB_NAME/config/prod.json" <<JSON
{
  "prod": {
    "source_paths": {
      "primary": "s3://YOUR-PROD-BUCKET/${ds}/*.csv"
    },
    "target_path": "s3://YOUR-PROD-BUCKET/out/${JOB_NAME}/",
    "repartition": 16,
    "partition_col": "${PARTITION_COL}",
    "join_key": "${JOIN_KEY}"
  }
}
JSON
}

emit_two_source_configs() {
  IFS=',' read -r SRC1 SRC2 <<< "$TWO_SOURCE"
  SRC1="${SRC1:-left}"
  SRC2="${SRC2:-right}"
  cat > "$ROOT/jobs/$JOB_NAME/config/dev.json" <<JSON
{
  "dev": {
    "source_paths": {
      "left":  "file:///ws/data/${SRC1}/dev/*.csv",
      "right": "file:///ws/data/${SRC2}/dev/*.csv"
    },
    "target_path": "file:///ws/out/${JOB_NAME}/dev/",
    "repartition": 4,
    "partition_col": "${PARTITION_COL}",
    "join_key": "${JOIN_KEY}"
  }
}
JSON

  cat > "$ROOT/jobs/$JOB_NAME/config/prod.json" <<JSON
{
  "prod": {
    "source_paths": {
      "left":  "s3://YOUR-PROD-BUCKET/${SRC1}/*.csv",
      "right": "s3://YOUR-PROD-BUCKET/${SRC2}/*.csv"
    },
    "target_path": "s3://YOUR-PROD-BUCKET/out/${JOB_NAME}/",
    "repartition": 16,
    "partition_col": "${PARTITION_COL}",
    "join_key": "${JOIN_KEY}"
  }
}
JSON
}

# ------------------------------
# TEST templates
# ------------------------------
emit_single_source_test() {
  cat > "$ROOT/tests/unit/test_${JOB_NAME}.py" <<'PY'
import json, sys, runpy
from pathlib import Path

def test_e2e_JOBNAME_single(spark, tmp_path):
    data_dir = tmp_path / "data" / "DATASET_PLACEHOLDER" / "dev"
    out_dir  = tmp_path / "out" / "JOBNAME" / "dev"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "sample.csv").write_text(
        "JOIN_KEY_PLACEHOLDER,value,PARTITION_PLACEHOLDER\n"
        "1,alpha,2025-11-01\n"
        "2,beta,2025-11-02\n"
    )

    cfg = {
        "dev": {
            "source_paths": { "primary": f"file://{data_dir}/*.csv" },
            "target_path":  f"file://{out_dir}/",
            "repartition":  2,
            "partition_col": "PARTITION_PLACEHOLDER",
            "join_key": "JOIN_KEY_PLACEHOLDER"
        }
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    argv_bak = sys.argv[:]
    sys.argv = ["pytest","--ENV","dev","--CONFIG_S3_URI",f"file://{cfg_path}","--BOOKMARKED","false"]
    try:
        runpy.run_path(str(Path("jobs/JOBNAME/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    df = spark.read.parquet(str(out_dir))
    assert df.count() == 2
PY
  python3 - "$ROOT/tests/unit/test_${JOB_NAME}.py" <<PY
import sys,pathlib
p=pathlib.Path(sys.argv[1]); s=p.read_text()
s=s.replace("JOBNAME","${JOB_NAME}")
s=s.replace("DATASET_PLACEHOLDER","${DATASET:-example}")
s=s.replace("PARTITION_PLACEHOLDER","${PARTITION_COL}")
s=s.replace("JOIN_KEY_PLACEHOLDER","${JOIN_KEY}")
p.write_text(s)
PY
}

emit_two_source_test() {
  IFS=',' read -r SRC1 SRC2 <<< "$TWO_SOURCE"
  cat > "$ROOT/tests/unit/test_${JOB_NAME}.py" <<'PY'
import json, sys, runpy
from pathlib import Path

def test_e2e_JOBNAME_two_sources(spark, tmp_path):
    left_dir  = tmp_path / "data" / "SRC1_PLACEHOLDER" / "dev"
    right_dir = tmp_path / "data" / "SRC2_PLACEHOLDER" / "dev"
    out_dir   = tmp_path / "out" / "JOBNAME" / "dev"
    left_dir.mkdir(parents=True, exist_ok=True)
    right_dir.mkdir(parents=True, exist_ok=True)

    (left_dir / "left.csv").write_text(
        "JOIN_KEY_PLACEHOLDER,order_dt,value\n"
        "1,2025-11-01,alpha\n"
        "2,2025-11-02,beta\n"
    )
    (right_dir / "right.csv").write_text(
        "JOIN_KEY_PLACEHOLDER,email\n"
        "1,ALICE@EXAMPLE.COM\n"
        "2,bob@example.com\n"
    )

    cfg = {
        "dev": {
            "source_paths": {
                "left":  f"file://{left_dir}/*.csv",
                "right": f"file://{right_dir}/*.csv"
            },
            "target_path":  f"file://{out_dir}/",
            "repartition":  2,
            "partition_col": "PARTITION_PLACEHOLDER",
            "join_key": "JOIN_KEY_PLACEHOLDER"
        }
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    argv_bak = sys.argv[:]
    sys.argv = ["pytest","--ENV","dev","--CONFIG_S3_URI",f"file://{cfg_path}","--BOOKMARKED","false"]
    try:
        runpy.run_path(str(Path("jobs/JOBNAME/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    df = spark.read.parquet(str(out_dir))
    # two orders, both should be present; email normalized
    assert df.count() == 2
    emails = {r.email for r in df.select("email").collect() if hasattr(r, "email")}
    assert emails == {"alice@example.com", "bob@example.com"}
PY
  python3 - "$ROOT/tests/unit/test_${JOB_NAME}.py" <<PY
import sys,pathlib
p=pathlib.Path(sys.argv[1]); s=p.read_text()
s=s.replace("JOBNAME","${JOB_NAME}")
s=s.replace("SRC1_PLACEHOLDER","${SRC1}")
s=s.replace("SRC2_PLACEHOLDER","${SRC2}")
s=s.replace("PARTITION_PLACEHOLDER","${PARTITION_COL}")
s=s.replace("JOIN_KEY_PLACEHOLDER","${JOIN_KEY}")
p.write_text(s)
PY
}

# ------------------------------
# SEEDS (optional)
# ------------------------------
create_seed_if_missing() {
  local ds="$1"
  mkdir -p "$ROOT/seeds"
  local f="$ROOT/seeds/${ds}.csv"
  if [[ -f "$f" ]]; then
    echo "[SKIP] $f already exists"
    return
  fi

  # Heuristic: if dataset name suggests customers, include email; else generic
  if [[ "$ds" == *"customer"* || "$ds" == *"customers"* ]]; then
    cat > "$f" <<CSV
${JOIN_KEY},email,${PARTITION_COL}
1,ALICE@EXAMPLE.COM,2025-11-01
2,bob@example.com,2025-11-02
CSV
  else
    cat > "$f" <<CSV
${JOIN_KEY},value,${PARTITION_COL}
1,alpha,2025-11-01
2,beta,2025-11-02
CSV
  fi
  echo "[OK] Created $f"
}

# ------------------------------
# Emit files per mode
# ------------------------------
if [[ -n "$TWO_SOURCE" ]]; then
  emit_two_source_main
  emit_two_source_configs
  emit_two_source_test
  if (( DO_SEED == 1 )); then
    IFS=',' read -r S1 S2 <<< "$TWO_SOURCE"
    create_seed_if_missing "$S1"
    create_seed_if_missing "$S2"
  fi
else
  emit_single_source_main
  emit_single_source_configs
  emit_single_source_test
  if (( DO_SEED == 1 )); then
    create_seed_if_missing "$DATASET"
  fi
fi

echo "[DONE] Scaffolded job: ${JOB_NAME}"
echo "-> Edit jobs/${JOB_NAME}/main.py and configs as needed."
if [[ -n "$TWO_SOURCE" ]]; then
  IFS=',' read -r S1 S2 <<< "$TWO_SOURCE"
  echo "-> To seed local data:   make seed-${S1}-dev ; make seed-${S2}-dev (or place CSVs in data/<dataset>/dev/)"
else
  echo "-> To seed local data:   make seed-${DATASET}-dev (or place CSVs in data/<dataset>/dev/)"
fi
echo "-> To run locally:       make run JOB=${JOB_NAME} ENV=dev"
echo "-> To run tests:         make test"
