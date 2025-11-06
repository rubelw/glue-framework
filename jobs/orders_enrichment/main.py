#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession, functions as F

# ---- Import shared libraries from /lib ----
from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats


# -----------------------------
# Helpers for JDBC / Db2
# -----------------------------
def _jdbc_url(host: str, port: int, database: str) -> str:
    return f"jdbc:db2://{host}:{port}/{database}"

def _normalize_columns_lower(df):
    """Downcase all column names to keep downstream checks stable."""
    for c in df.columns:
        if c != c.lower():
            df = df.withColumnRenamed(c, c.lower())
    return df

def _read_db2_table(spark: SparkSession, db2_cfg: dict):
    """
    Read a Db2 table with Spark JDBC.
    Required keys: host, port, database, schema, table, user, password
    Optional: driver_class, jdbc_jar, partition{column,lower_bound,upper_bound,num_partitions}, predicates
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

    # Attach jar at runtime if provided (harmless if also passed via --jars)
    if jdbc_jar:
        spark.conf.set("spark.jars", jdbc_jar)

    url = _jdbc_url(host, port, database)
    dbtable = f"{schema}.{table}"

    opts = {
        "url": url,
        "dbtable": dbtable,
        "user": user,
        "password": password,
        "driver": driver,
        "fetchsize": "1000",
    }

    # Parallel read support (range partitioning)
    part = db2_cfg.get("partition", {})
    if all(k in part for k in ("column", "lower_bound", "upper_bound")):
        opts.update({
            "partitionColumn": str(part["column"]),
            "lowerBound": str(part["lower_bound"]),
            "upperBound": str(part["upper_bound"]),
            "numPartitions": str(part.get("num_partitions", 4)),
        })

    # Optional predicate pushdown via an array of predicates (mutually exclusive with range partitioning)
    predicates = db2_cfg.get("predicates")
    if predicates and isinstance(predicates, list) and len(predicates) > 0:
        print(f"[INFO] Reading Db2 via JDBC (predicates) url={url} table={dbtable}")
        df = spark.read.format("jdbc").option("url", url) \
            .option("dbtable", dbtable).option("user", user).option("password", password) \
            .option("driver", driver).option("fetchsize", "1000") \
            .option("pushDownPredicate", "true") \
            .load(predicates=predicates)  # type: ignore[attr-defined] (supported in Spark's Scala; Python passes through)
    else:
        print(f"[INFO] Reading Db2 via JDBC: {url} table={dbtable}")
        df = spark.read.format("jdbc").options(**opts).load()

    return _normalize_columns_lower(df)


def _read_side(spark: SparkSession, cfg: dict, side: str):
    """
    Read a side ("left" or "right") from either Db2 (if present) or CSV (fallback).
    Db2 config keys are 'left_db2' and 'right_db2'.
    CSV fallback key is cfg['source_paths'][side].
    """
    db2_key = f"{side}_db2"
    if isinstance(cfg.get(db2_key), dict):
        return _read_db2_table(spark, cfg[db2_key])

    # CSV fallback
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
# ⚙️ STEP 2: Load configuration for the selected environment
# ============================================================
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "order_dt")
join_key     = cfg.get("join_key", "customer_id")

# Create output dir in local (file://) mode
ensure_dir(target_path, scheme="file")

# ============================================================
# 📥 STEP 4: Read source data (Db2 if configured; else CSV)
# ============================================================
left  = _read_side(spark, cfg, "left")
right = _read_side(spark, cfg, "right")

# ============================================================
# 🧪 STEP 5: Validate schema and required columns
# ============================================================
req_left  = [join_key] + ([partition_by] if partition_by else [])
req_right = [join_key]
require_columns(left,  req_left)
require_columns(right, req_right)

# ============================================================
# 🧹 STEP 6: Data standardization and cleanup
# ============================================================
if "email" in left.columns:
    left = normalize_lower(left, "email")
if "email" in right.columns:
    right = normalize_lower(right, "email")

right = dedupe_by_keys(right, [join_key])

# ============================================================
# 🔗 STEP 7: Join datasets and enrich
# ============================================================
enriched = (
    left.join(F.broadcast(right), on=join_key, how="left")
        .withColumn("load_ts", F.current_timestamp())
)

# ============================================================
# 💾 STEP 8: Write enriched results to output
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
    job="orders_enrichment_jdbc" if ("left_db2" in cfg or "right_db2" in cfg) else "orders_enrichment"
)
