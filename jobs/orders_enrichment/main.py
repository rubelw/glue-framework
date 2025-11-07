#!/usr/bin/env python3
import sys
import os
import socket
from urllib.parse import urlparse
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

# ---- Import shared libraries from /lib ----
from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats


# -----------------------------
# Spark/JDBC bootstrap (Db2 + Postgres)
# -----------------------------
def _collect_jdbc_jars():
    """
    Gather JDBC jars from env or common locations.
      - DB2_JAR: absolute path to db2jcc4.jar
      - PG_JAR:  path to postgresql-*.jar
    """
    candidates = []

    # Explicit env overrides
    for key in ("DB2_JAR", "PG_JAR"):
        p = os.environ.get(key)
        if p:
            candidates.append(p)

    # Project-local ./jars (explicit common names)
    for name in (
        "db2jcc4.jar",
        "db2jcc.jar",
        "postgresql.jar",
        "postgresql-42.7.4.jar",
        "postgresql-42.7.3.jar",
    ):
        candidates.append(os.path.abspath(os.path.join(os.getcwd(), "jars", name)))

    # Project-local ./jars (wildcard scan)
    jars_dir = os.path.abspath(os.path.join(os.getcwd(), "jars"))
    if os.path.isdir(jars_dir):
        for fname in os.listdir(jars_dir):
            if not fname.endswith(".jar"):
                continue
            if ("postgresql-" in fname) or ("db2jcc" in fname):
                candidates.append(os.path.join(jars_dir, fname))

    # Fallback common Db2 locations
    candidates.extend([
        "/opt/jars/db2jcc4.jar",
        "/opt/ibm/db2/jdbc/db2jcc4.jar",
        "/opt/ibm/db2/V11.5/java/db2jcc4.jar",
        "/usr/share/java/db2jcc4.jar",
        "/usr/lib/jvm/db2jcc4.jar",
    ])

    # Keep only existing files; de-dupe preserving order
    seen, jars = set(), []
    for p in candidates:
        ap = os.path.abspath(p)
        if os.path.exists(ap) and ap not in seen:
            jars.append(ap)
            seen.add(ap)
    return jars


def _jclass_exists(spark, fqcn: str) -> bool:
    try:
        spark._jvm.java.lang.Class.forName(fqcn)
        return True
    except Exception:
        return False


def _attach_driver_jars_runtime(spark, jars: list[str]) -> None:
    """
    Best-effort: make the running driver & executors see these jars
    without restarting the SparkSession.
    """
    # Tell executors
    try:
        jsc = spark.sparkContext._jsc
        for p in jars:
            try:
                jsc.addJar(p)
            except Exception:
                pass
    except Exception:
        pass

    # Driver: reflect URLClassLoader.addURL
    try:
        jvm = spark._jvm
        File = jvm.java.io.File
        URL = jvm.java.net.URL
        URLClassLoader = jvm.java.net.URLClassLoader
        Thread = jvm.java.lang.Thread

        cl = Thread.currentThread().getContextClassLoader()
        method = URLClassLoader.getDeclaredMethod("addURL", [URL])
        method.setAccessible(True)
        for p in jars:
            method.invoke(cl, [File(p).toURI().toURL()])
    except Exception:
        pass


def build_spark():
    jars = _collect_jdbc_jars()

    # If no session yet, create one with jars
    spark = SparkSession.getActiveSession()
    if spark is None:
        if jars:
            csv = ",".join(jars)
            print(f"[INFO] Spark JDBC jars (cold start): {csv}")
            return (
                SparkSession.builder
                .config("spark.jars", csv)
                .config("spark.driver.extraClassPath", csv)
                .config("spark.executor.extraClassPath", csv)
                .getOrCreate()
            )
        return SparkSession.builder.getOrCreate()

    # Session already exists (pytest fixture). Do NOT stop it.
    need_pg = not _jclass_exists(spark, "org.postgresql.Driver")
    need_db2 = not _jclass_exists(spark, "com.ibm.db2.jcc.DB2Driver")

    if need_pg or need_db2:
        print("[INFO] Active SparkSession missing JDBC driver(s):"
              f"{' PG' if need_pg else ''}{' DB2' if need_db2 else ''}. Attaching jars at runtime…")
        if jars:
            _attach_driver_jars_runtime(spark, jars)

        # re-probe
        need_pg = not _jclass_exists(spark, "org.postgresql.Driver")
        need_db2 = not _jclass_exists(spark, "com.ibm.db2.jcc.DB2Driver")

        if need_pg or need_db2:
            missing = []
            if need_pg:
                missing.append("org.postgresql.Driver (set PG_JAR to postgresql-*.jar)")
            if need_db2:
                missing.append("com.ibm.db2.jcc.DB2Driver (set DB2_JAR to db2jcc4.jar)")
            print("[WARN] Spark still can’t see: " + ", ".join(missing))
            print("[WARN] Continuing; JDBC call will fail with ClassNotFoundException if truly absent.")
    return spark


# -----------------------------
# Helpers for JDBC / Db2
# -----------------------------
def _jdbc_url(host: str, port: int, database: str) -> str:
    return f"jdbc:db2://{host}:{port}/{database}"


def _normalize_columns_lower(df):
    """Downcase all column names to keep downstream checks stable."""
    return df.select([col(c).alias(c.lower()) for c in df.columns])


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

    # Optional predicate pushdown
    predicates = db2_cfg.get("predicates")
    if predicates and isinstance(predicates, list) and len(predicates) > 0:
        print(f"[INFO] Reading Db2 via JDBC (predicates) url={url} table={dbtable}")
        df = (spark.read.format("jdbc")
              .option("url", url)
              .option("dbtable", dbtable)
              .option("user", user)
              .option("password", password)
              .option("driver", driver)
              .option("fetchsize", "1000")
              .option("pushDownPredicate", "true")
              .load(predicates=predicates))  # type: ignore[attr-defined]
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


# -----------------------------
# Utilities for network + Postgres
# -----------------------------
def _tcp_ready(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _jdbc_url_generic(dbtype: str, host: str, port: str | int, db: str) -> str:
    return f"jdbc:{dbtype}://{host}:{port}/{db}"


def _apply_pg_overrides(pg_cfg: dict) -> dict:
    """Allow env to override Postgres endpoint."""
    pg_host = os.environ.get("PG_HOST")
    pg_port = os.environ.get("PG_PORT")
    pg_db = os.environ.get("PG_DB")
    pg_user = os.environ.get("PG_USER")
    pg_pwd = os.environ.get("PG_PASSWORD")

    if any([pg_host, pg_port, pg_db]):
        try:
            parsed = urlparse(pg_cfg["url"].replace("jdbc:postgresql://", "postgresql://"))
            cur_host = parsed.hostname or "127.0.0.1"
            cur_port = parsed.port or 5432
            cur_db = (parsed.path or "/postgres").lstrip("/")
        except Exception:
            cur_host, cur_port, cur_db = ("127.0.0.1", 5432, "postgres")
        new_url = _jdbc_url_generic("postgresql", pg_host or cur_host, pg_port or cur_port, pg_db or cur_db)
        pg_cfg = {**pg_cfg, "url": new_url}
    if pg_user:
        pg_cfg = {**pg_cfg, "user": pg_user}
    if pg_pwd:
        pg_cfg = {**pg_cfg, "password": pg_pwd}
    return pg_cfg


def write_to_postgres(df, pg_cfg: dict):
    """Write a DataFrame to Postgres via JDBC (lowercase, unquoted identifiers)."""
    # Connectivity preflight (nice error if host/port wrong)
    try:
        parsed = urlparse(pg_cfg["url"].replace("jdbc:postgresql://", "postgresql://"))
        _host = parsed.hostname
        _port = parsed.port or 5432
        if not _tcp_ready(_host, _port):
            raise RuntimeError(
                f"Postgres not reachable at {_host}:{_port}. "
                "Set PG_HOST/PG_PORT correctly (use host.docker.internal from Docker on macOS) "
                "and ensure the service is listening."
            )
    except Exception:
        pass

    dbtable = pg_cfg.get("dbtable") or pg_cfg.get("table")
    if not dbtable:
        raise KeyError("postgres config must include 'dbtable' or 'table'")
    mode = pg_cfg.get("mode", "append")
    batch_size = int(pg_cfg.get("batch_size", 5000))
    isolation = pg_cfg.get("isolation", "READ_COMMITTED")

    df_out = _normalize_columns_lower(df)
    (
        df_out.write
        .format("jdbc")
        .mode(mode)
        .option("url", pg_cfg["url"])
        .option("dbtable", dbtable)
        .option("user", pg_cfg["user"])
        .option("password", pg_cfg["password"])
        .option("driver", pg_cfg.get("driver", "org.postgresql.Driver"))
        .option("quoteIdentifier", "false")  # let PG fold to lowercase
        .option("truncate", "false")
        .option("batchsize", str(batch_size))
        .option("isolationLevel", isolation)
        .save()
    )
    return dbtable, mode


# ============================================================
# 🧭 STEP 1: Parse runtime arguments and initialize Spark
# ============================================================
args = parse_args(sys.argv)
spark = build_spark()

# ============================================================
# ⚙️ STEP 2: Load configuration for the selected environment
# ============================================================
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

target_path = cfg["target_path"]
repartition = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "order_dt")
join_key = cfg.get("join_key", "customer_id")

# Create output dir in local (file://) mode
ensure_dir(target_path, scheme="file")

# ============================================================
# 📥 STEP 4: Read source data (Db2 if configured; else CSV)
# ============================================================
left = _read_side(spark, cfg, "left")
right = _read_side(spark, cfg, "right")

# Normalize columns early for stable downstream logic
left = _normalize_columns_lower(left)
right = _normalize_columns_lower(right)

# ============================================================
# 🧪 STEP 5: Validate schema and required columns
# ============================================================
req_left = [join_key] + ([partition_by] if partition_by else [])
req_right = [join_key]
require_columns(left, req_left)
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

# --- NEW: conform schema expected by tests ---
# 1) Rename 'value' -> 'amount' if present
if "value" in enriched.columns and "amount" not in enriched.columns:
    enriched = enriched.withColumnRenamed("value", "amount")

# 2) Add a deterministic order_id if missing
#    Use a stable, readable surrogate: customer_id + order_dt
if "order_id" not in enriched.columns:
    # Ensure the source columns exist before building order_id
    if all(c in enriched.columns for c in ["customer_id", "order_dt"]):
        enriched = enriched.withColumn(
            "order_id",
            F.concat_ws("-", F.col("customer_id").cast("string"), F.col("order_dt").cast("string"))
        )
    else:
        # Fallback: generate a synthetic but stable-ish id (monotonic within partition)
        enriched = enriched.withColumn("order_id", F.monotonically_increasing_id().cast("string"))

# ============================================================
# 💾 STEP 8: Write enriched results (Parquet or Postgres)
# ============================================================
pg_cfg = cfg.get("postgres")
if isinstance(pg_cfg, dict):
    pg_cfg = _apply_pg_overrides(pg_cfg)
    fq_table, mode = write_to_postgres(enriched, pg_cfg)
    sink_target = f'{pg_cfg["url"]}#{fq_table}'
    sink_job = f'orders_enrichment_to_postgres[{mode}]'
else:
    write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)
    sink_target = target_path
    sink_job = "orders_enrichment"

# ============================================================
# 📊 STEP 9: Emit run statistics
# ============================================================
emit_run_stats(
    env=env,
    rows_in_left=left.count(),
    rows_in_right=right.count(),
    rows_out=enriched.count(),
    target=sink_target,
    job=sink_job
)
