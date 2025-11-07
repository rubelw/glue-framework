#!/usr/bin/env python3
import sys, os, socket
from urllib.parse import urlparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

# ---- Import shared framework libraries ----
from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats


# ============================
# JDBC jar collection / Spark
# ============================
def _collect_jdbc_jars():
    """
    Collect JDBC jars from env or common locations.
    - DB2_JAR: absolute path to db2jcc4.jar (Db2 driver is not on Maven Central)
    - PG_JAR:  optional PostgreSQL driver path
    Also scan ./jars and a few common Db2 install paths.
    """
    candidates = []

    # Explicit env overrides
    for key in ("DB2_JAR", "PG_JAR"):
        p = os.environ.get(key)
        if p:
            candidates.append(p)

    # Project-local ./jars (explicit names)
    for name in (
        "db2jcc4.jar",
        "db2jcc.jar",
        "postgresql.jar",
        "postgresql-42.7.4.jar",
        "postgresql-42.7.3.jar",
    ):
        candidates.append(os.path.abspath(os.path.join(os.getcwd(), "jars", name)))

    # Project-local ./jars wildcard scan
    jars_dir = os.path.abspath(os.path.join(os.getcwd(), "jars"))
    if os.path.isdir(jars_dir):
        for fname in os.listdir(jars_dir):
            if fname.endswith(".jar") and (("postgresql-" in fname) or ("db2jcc" in fname)):
                candidates.append(os.path.join(jars_dir, fname))

    # Fallback common Db2 locations
    candidates.extend([
        "/opt/jars/db2jcc4.jar",
        "/opt/ibm/db2/jdbc/db2jcc4.jar",
        "/opt/ibm/db2/V11.5/java/db2jcc4.jar",
        "/usr/share/java/db2jcc4.jar",
        "/usr/lib/jvm/db2jcc4.jar",
    ])

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
    # 1) Tell executors
    try:
        jsc = spark.sparkContext._jsc
        for p in jars:
            try:
                jsc.addJar(p)
            except Exception:
                pass
    except Exception:
        pass

    # 2) Try to put jars on the driver's classloader (reflection)
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
            url = File(p).toURI().toURL()
            method.invoke(cl, [url])
    except Exception:
        pass


def _force_rebuild_with_jars(jars: list[str]):
    csv = ",".join(jars)
    return (
        SparkSession.builder
        .config("spark.jars", csv)
        .config("spark.driver.extraClassPath", csv)
        .config("spark.executor.extraClassPath", csv)
        .getOrCreate()
    )


def build_spark():
    jars = _collect_jdbc_jars()

    spark = SparkSession.getActiveSession()
    if spark is None:
        if jars:
            print(f"[INFO] Spark JDBC jars (cold start): {','.join(jars)}")
            return _force_rebuild_with_jars(jars)
        return SparkSession.builder.getOrCreate()

    need_pg  = not _jclass_exists(spark, "org.postgresql.Driver")
    need_db2 = not _jclass_exists(spark, "com.ibm.db2.jcc.DB2Driver")

    if need_pg or need_db2:
        print("[INFO] Active SparkSession missing JDBC driver(s):"
              f"{' PG' if need_pg else ''}{' DB2' if need_db2 else ''}. Attaching jars at runtime…")
        if jars:
            _attach_driver_jars_runtime(spark, jars)

        need_pg  = not _jclass_exists(spark, "org.postgresql.Driver")
        need_db2 = not _jclass_exists(spark, "com.ibm.db2.jcc.DB2Driver")
        if not (need_pg or need_db2):
            return spark

        missing = []
        if need_pg:  missing.append("org.postgresql.Driver (set PG_JAR to a postgresql-*.jar)")
        if need_db2: missing.append("com.ibm.db2.jcc.DB2Driver (set DB2_JAR to db2jcc4.jar)")
        print("[WARN] Spark still can’t see: " + ", ".join(missing))
        print("[WARN] Continuing; JDBC call will fail with ClassNotFoundException if truly absent.")
        return spark

    return spark


# ======================
# Db2 / JDBC helpers
# ======================
def _tcp_ready(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _jdbc_url(host: str, port: int, database: str) -> str:
    return f"jdbc:db2://{host}:{port}/{database}"


def _normalize_columns_lower(df):
    """Downcase all column names so DQ helpers (require_columns, etc.) stay consistent."""
    return df.select([col(c).alias(c.lower()) for c in df.columns])


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

    # Allow env overrides
    host = os.environ.get("DB2_HOST", str(db2_cfg["host"]))
    port = int(os.environ.get("DB2_PORT", str(db2_cfg["port"])))
    database = os.environ.get("DB2_DB", str(db2_cfg["database"]))
    schema = db2_cfg["schema"]
    table = db2_cfg["table"]
    user = os.environ.get("DB2_USER", str(db2_cfg["user"]))
    password = os.environ.get("DB2_PASSWORD", str(db2_cfg["password"]))
    driver = db2_cfg.get("driver_class", "com.ibm.db2.jcc.DB2Driver")
    jdbc_jar = db2_cfg.get("jdbc_jar")

    # Attach the driver jar if provided (OK if also passed via --jars)
    if jdbc_jar:
        spark.conf.set("spark.jars", jdbc_jar)

    # Quick connectivity check to make failures clearer
    if not _tcp_ready(host, port):
        raise RuntimeError(f"Db2 not reachable at {host}:{port}. Set DB2_HOST/DB2_PORT correctly and ensure service is listening.")

    url = _jdbc_url(host, port, database)
    dbtable = f"{schema}.{table}"

    reader = (spark.read.format("jdbc")
              .option("url", url)
              .option("dbtable", dbtable)
              .option("user", user)
              .option("password", password)
              .option("driver", driver)
              .option("fetchsize", "1000"))

    part = db2_cfg.get("partition", {})
    if all(k in part for k in ("column", "lower_bound", "upper_bound")):
        reader = (reader
                  .option("partitionColumn", str(part["column"]))
                  .option("lowerBound", str(part["lower_bound"]))
                  .option("upperBound", str(part["upper_bound"]))
                  .option("numPartitions", str(part.get("num_partitions", 4))))

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


# ======================
# Postgres sink helpers
# ======================
def _pg_apply_overrides(pg_cfg: dict) -> dict:
    """
    Allow PG_* env vars to override postgres connection.
      PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    """
    if not isinstance(pg_cfg, dict):
        return pg_cfg

    try:
        parsed = urlparse(pg_cfg["url"].replace("jdbc:postgresql://", "postgresql://"))
        cur_host = parsed.hostname or "127.0.0.1"
        cur_port = parsed.port or 5432
        cur_db   = (parsed.path or "/postgres").lstrip("/")
    except Exception:
        cur_host, cur_port, cur_db = ("127.0.0.1", 5432, "postgres")

    host = os.environ.get("PG_HOST", str(cur_host))
    port = os.environ.get("PG_PORT", str(cur_port))
    db   = os.environ.get("PG_DB",   str(cur_db))
    user = os.environ.get("PG_USER", pg_cfg.get("user", "etl"))
    pwd  = os.environ.get("PG_PASSWORD", pg_cfg.get("password", ""))

    new_url = f"jdbc:postgresql://{host}:{port}/{db}"
    out = {**pg_cfg, "url": new_url, "user": user, "password": pwd}
    print(f"[INFO] Effective PG  URL: {out['url']}")
    return out


def _write_to_postgres(df, pg_cfg: dict):
    """
    Write a DataFrame to Postgres via JDBC.
    Required keys: url, user, password, and either 'table' or 'dbtable'.
    Optional: driver, mode (overwrite/append), batchsize, isolation.
    """
    pg_cfg = _pg_apply_overrides(pg_cfg)

    # Preflight connectivity check
    try:
        parsed = urlparse(pg_cfg["url"].replace("jdbc:postgresql://", "postgresql://"))
        _host = parsed.hostname or "127.0.0.1"
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
          .option("quoteIdentifier", "false")
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
# ⚙️ STEP 2: Load configuration for current environment
# ============================================================
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "txn_dt")
join_key     = cfg.get("join_key", "order_id")

# Local (file://) output safety for parquet fallback
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

# Optional: conform to expected names if your tests require (example)
# if "amount" not in enriched.columns and "value" in enriched.columns:
#     enriched = enriched.withColumnRenamed("value", "amount")

if repartition and repartition > 0:
    enriched = enriched.repartition(repartition)

# ============================================================
# 💾 STEP 8: Write results (to Postgres if configured; else parquet)
# ============================================================
pg_cfg = cfg.get("postgres")
if isinstance(pg_cfg, dict):
    fq_table, mode = _write_to_postgres(enriched, pg_cfg)
    sink_target = f"{pg_cfg.get('url', '')}#{fq_table}"
    sink_job = f"payments_recon_to_postgres[{mode}]"
else:
    write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)
    sink_target = target_path
    sink_job = "payments_recon_jdbc" if ("left_db2" in cfg or "right_db2" in cfg) else "payments_recon"

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
