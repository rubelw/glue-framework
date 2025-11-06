#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession, functions as F

# ---- Import shared framework libraries ----
from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats


# -----------------------------
# Db2 / JDBC helpers
# -----------------------------
def _jdbc_url(host: str, port: int, database: str) -> str:
    return f"jdbc:db2://{host}:{port}/{database}"

def _normalize_columns_lower(df):
    """Downcase all column names so DQ helpers (require_columns, etc.) stay consistent."""
    for c in df.columns:
        if c != c.lower():
            df = df.withColumnRenamed(c, c.lower())
    return df

def _read_db2_table(spark: SparkSession, db2_cfg: dict):
    """
    Read a Db2 table via JDBC.
    Required keys: host, port, database, schema, table, user, password
    Optional keys: driver_class, jdbc_jar, partition{column,lower_bound,upper_bound,num_partitions}, predicates
    """
    required = ["host", "port", "database", "schema", "table", "user", "password"]
    missing = [k for k in required if not db2_cfg.get(k)]
    if missing:
        raise RuntimeError(f"db2 config missing required keys: {missing}")

    host = db2_cfg["host"]
    port = int(db2_cfg["port"])
    database = db2_cfg["database"]
    schema = db2_cfg["schema"]
    table = db2_cfg["table"]
    user = db2_cfg["user"]
    password = db2_cfg["password"]
    driver = db2_cfg.get("driver_class", "com.ibm.db2.jcc.DB2Driver")
    jdbc_jar = db2_cfg.get("jdbc_jar")

    # Attach the driver jar if provided (OK if also passed via --jars)
    if jdbc_jar:
        spark.conf.set("spark.jars", jdbc_jar)

    url = _jdbc_url(host, port, database)
    dbtable = f"{schema}.{table}"

    reader = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .option("fetchsize", "1000")

    part = db2_cfg.get("partition", {})
    predicates = db2_cfg.get("predicates")

    # Prefer explicit predicates if provided (mutually exclusive with range partitioning)
    if predicates and isinstance(predicates, list) and len(predicates) > 0:
        print(f"[INFO] Reading Db2 with predicates: {dbtable}")
        df = reader.load()  # PySpark doesn't have load(predicates=...), so rely on WHERE in dbtable if needed
    else:
        if all(k in part for k in ("column", "lower_bound", "upper_bound")):
            reader = reader \
                .option("partitionColumn", str(part["column"])) \
                .option("lowerBound", str(part["lower_bound"])) \
                .option("upperBound", str(part["upper_bound"])) \
                .option("numPartitions", str(part.get("num_partitions", 4)))
        print(f"[INFO] Reading Db2 via JDBC: {url} table={dbtable}")
        df = reader.load()

    return _normalize_columns_lower(df)

def _read_side(spark: SparkSession, cfg: dict, side: str):
    """
    Read one side ('left' or 'right') from Db2 if configured, otherwise fallback to CSV.
    Db2 config keys: 'left_db2', 'right_db2'
    CSV fallback: cfg['source_paths'][side]
    """
    db2_key = f"{side}_db2"
    if isinstance(cfg.get(db2_key), dict):
        return _read_db2_table(spark, cfg[db2_key])

    src_paths = cfg.get("source_paths", {})
    path = src_paths.get(side)
    if not path:
        raise RuntimeError(f"No {db2_key} block and no source_paths['{side}'] configured.")
    must_exist_glob(path, label=f"source:{side}", if_scheme="file")
    return _normalize_columns_lower(read_csv(spark, path))


# ============================================================
# 🧭 STEP 1: Parse runtime arguments and initialize Spark
# ============================================================
args = parse_args(sys.argv)
spark = SparkSession.builder.getOrCreate()

# ============================================================
# ⚙️ STEP 2: Load configuration for current environment
# ============================================================
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "txn_dt")
join_key     = cfg.get("join_key", "order_id")

# Local (file://) output safety
ensure_dir(target_path, scheme="file")

# ============================================================
# 📥 STEP 4: Read input data (Db2 if configured; else CSV)
# ============================================================
left  = _read_side(spark, cfg, "left")
right = _read_side(spark, cfg, "right")

# ============================================================
# 🧪 STEP 5: Validate schema requirements
# ============================================================
req_left  = [join_key] + ([partition_by] if partition_by else [])
req_right = [join_key]
require_columns(left,  req_left)
require_columns(right, req_right)

# ============================================================
# 🧹 STEP 6: Clean and normalize data
# ============================================================
if "email" in left.columns:
    left = normalize_lower(left, "email")
if "email" in right.columns:
    right = normalize_lower(right, "email")

right = dedupe_by_keys(right, [join_key])

# ============================================================
# 🔗 STEP 7: Join and enrich data
# ============================================================
enriched = (
    left.join(F.broadcast(right), on=join_key, how="left")
        .withColumn("load_ts", F.current_timestamp())
)

# ============================================================
# 💾 STEP 8: Write results to output
# ============================================================
write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)

# ============================================================
# 📊 STEP 9: Emit run statistics
# ============================================================
emit_run_stats(
    env=env,
    rows_in_left=left.count(),
    rows_in_right=right.count(),
    rows_out=enriched.count(),
    target=target_path,
    job="payments_recon_jdbc" if ("left_db2" in cfg or "right_db2" in cfg) else "payments_recon"
)
