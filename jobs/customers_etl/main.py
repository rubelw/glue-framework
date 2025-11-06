#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession, functions as F

# ---- shared library imports ----
from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import dedupe_by_keys, normalize_lower, require_columns
from lib.logging import emit_run_stats

# -----------------------------
# Helpers for JDBC / Db2
# -----------------------------
def _jdbc_url(host: str, port: int, database: str) -> str:
    return f"jdbc:db2://{host}:{port}/{database}"

def _normalize_columns_lower(df):
    """Make downstream logic stable by lowercasing column names."""
    for c in df.columns:
        if c != c.lower():
            df = df.withColumnRenamed(c, c.lower())
    return df

def _read_db2_table(spark: SparkSession, db2_cfg: dict):
    """
    Read a Db2 table with Spark JDBC using options from db2_cfg.
    Expected keys:
      host, port, database, schema, table, user, password
      (optional) driver_class, jdbc_jar, partition{column,lower_bound,upper_bound,num_partitions}
    """
    required = ["host", "port", "database", "schema", "table", "user", "password"]
    missing = [k for k in required if k not in db2_cfg or db2_cfg[k] in (None, "")]
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

    # If provided, attach the jar at runtime (useful for local runs)
    builder = spark
    if jdbc_jar:
        # If you already launch with --jars, this is harmless
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

    # Optional parallel read
    part = db2_cfg.get("partition", {})
    if all(k in part for k in ("column", "lower_bound", "upper_bound")):
        opts.update({
            "partitionColumn": str(part["column"]),
            "lowerBound": str(part["lower_bound"]),
            "upperBound": str(part["upper_bound"]),
            "numPartitions": str(part.get("num_partitions", 4)),
        })

    print(f"[INFO] Reading Db2 via JDBC: {url} table={dbtable}")
    df = spark.read.format("jdbc").options(**opts).load()
    # Standardize names to lowercase for downstream steps
    return _normalize_columns_lower(df)

# ============================================================
# 🧭 STEP 1: Parse runtime arguments and start Spark
# ============================================================
args = parse_args(sys.argv)
spark = SparkSession.builder.getOrCreate()

# ============================================================
# ⚙️ STEP 2: Load configuration
# ============================================================
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

# Output & tuning
tgt_path  = cfg["target_path"]
repart    = int(cfg.get("repartition", 8))
partition = cfg.get("partition_col", "ingest_dt")

# ============================================================
# 📥 STEP 3/4: Read input data (Db2 JDBC if configured, else CSV)
# ============================================================
use_db2 = isinstance(cfg.get("db2"), dict)

if use_db2:
    df = _read_db2_table(spark, cfg["db2"])
else:
    # CSV mode (original behavior)
    src_path  = cfg["source_path"]
    must_exist_glob(src_path, label="source_path", if_scheme="file")
    ensure_dir(tgt_path, scheme="file")
    df = read_csv(spark, src_path)
    # For CSVs you already get lowercase headers from your source; keep consistent anyway
    df = _normalize_columns_lower(df)

# ============================================================
# 🧪 STEP 5: Validate schema and required columns
# ============================================================
required_cols = ["customer_id", "email"]
if partition:
    required_cols.append(partition)
require_columns(df, required_cols)

# ============================================================
# 🧹 STEP 6: Transformations
# ============================================================
df = dedupe_by_keys(df, ["customer_id"])
df = normalize_lower(df, "email")
df = df.withColumn("load_ts", F.current_timestamp())

# ============================================================
# 💾 STEP 7: Write results
# ============================================================
write_parquet(df, tgt_path, repartition=repart, partitionBy=partition)

# ============================================================
# 📊 STEP 8: Emit run statistics
# ============================================================
emit_run_stats(
    env=env,
    rows_out=df.count(),
    target=tgt_path,
    job="customers_etl" if not use_db2 else f"customers_etl_jdbc_{cfg['db2']['table'].lower()}"
)
