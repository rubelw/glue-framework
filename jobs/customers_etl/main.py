import sys
import os
from pyspark.sql import SparkSession, functions as F
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import col


import socket
from urllib.parse import urlparse

# shared libs you already have
from lib.args import parse_args
from lib.config import load_config
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats


# inside jobs/customers_etl/main.py

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

    # Project-local ./jars (explicit common names)
    for name in (
        "db2jcc4.jar",
        "db2jcc.jar",
        "postgresql.jar",
        "postgresql-42.7.4.jar",
        "postgresql-42.7.3.jar",
    ):
        candidates.append(os.path.abspath(os.path.join(os.getcwd(), "jars", name)))

    # Project-local ./jars (wildcard scan so any postgresql-*.jar/db2jcc*.jar are picked up)
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

def _force_rebuild_with_jars(jars: list[str]):
    csv = ",".join(jars)
    builder = (
        SparkSession.builder
        .config("spark.jars", csv)
        .config("spark.driver.extraClassPath", csv)
        .config("spark.executor.extraClassPath", csv)
    )
    print(f"[INFO] Spark JDBC jars: {csv}")
    return builder.getOrCreate()

# --- add these helpers near build_spark() ---

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

    # 2) Try to put jars on the driver's classloader (URLClassLoader.addURL via reflection)
    try:
        jvm = spark._jvm
        File = jvm.java.io.File
        URL = jvm.java.net.URL
        URLClassLoader = jvm.java.net.URLClassLoader
        Thread = jvm.java.lang.Thread
        Class = jvm.java.lang.Class

        cl = Thread.currentThread().getContextClassLoader()
        # URLClassLoader.addURL(URL) is protected; reflect it open
        method = URLClassLoader.getDeclaredMethod("addURL", [URL])
        method.setAccessible(True)
        for p in jars:
            url = File(p).toURI().toURL()
            method.invoke(cl, [url])
    except Exception:
        # If reflection doesn’t work on this JVM, we still tried sc.addJar above.
        pass

def force_rebuild_with_jars(jars: list[str]):
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

    # If no session yet, create one with jars
    spark = SparkSession.getActiveSession()
    if spark is None:
        if jars:
            print(f"[INFO] Spark JDBC jars (cold start): {','.join(jars)}")
            return _force_rebuild_with_jars(jars)
        return SparkSession.builder.getOrCreate()

    # Session already exists (pytest fixture). Do NOT stop it.
    need_pg  = not _jclass_exists(spark, "org.postgresql.Driver")
    need_db2 = not _jclass_exists(spark, "com.ibm.db2.jcc.DB2Driver")

    if need_pg or need_db2:
        # try to hot-attach jars
        print("[INFO] Active SparkSession missing JDBC driver(s):"
              f"{' PG' if need_pg else ''}{' DB2' if need_db2 else ''}. Attaching jars at runtime…")
        if jars:
            _attach_driver_jars_runtime(spark, jars)

        # re-probe
        need_pg  = not _jclass_exists(spark, "org.postgresql.Driver")
        need_db2 = not _jclass_exists(spark, "com.ibm.db2.jcc.DB2Driver")

        if not (need_pg or need_db2):
            return spark

        # Still “missing”: warn but DO NOT raise here.
        missing = []
        if need_pg:  missing.append("org.postgresql.Driver (set PG_JAR to a postgresql-*.jar)")
        if need_db2: missing.append("com.ibm.db2.jcc.DB2Driver (set DB2_JAR to db2jcc4.jar)")
        print("[WARN] Spark running but our driver probe still can’t see: " + ", ".join(missing))
        print("[WARN] Continuing. If they’re truly absent, the JDBC call will fail with a clear ClassNotFoundException.")
        return spark

    return spark



def _tcp_ready(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def read_from_db2(spark, db2_cfg):
    """
    Read a table (or subquery) from Db2 via JDBC.
    Required keys: url, user, password, dbtable|table
    Optional: driver, fetch_size, partitioning{column, lower_bound, upper_bound, num_partitions}
    """
    # accept either "dbtable" or "table"
    dbtable = db2_cfg.get("dbtable") or db2_cfg.get("table")
    if not dbtable:
        raise KeyError("db2 config must include 'dbtable' or 'table'")

    reader = (
        spark.read.format("jdbc")
             .option("url", db2_cfg["url"])
             .option("dbtable", dbtable)  # e.g. "DB2INST1.CUSTOMERS_DEV" or "(select ..) t"
             .option("user", db2_cfg["user"])
             .option("password", db2_cfg["password"])
             .option("driver", db2_cfg.get("driver", "com.ibm.db2.jcc.DB2Driver"))
    )

    if "fetch_size" in db2_cfg:
        reader = reader.option("fetchsize", str(db2_cfg["fetch_size"]))

    # Optional parallel read
    part = db2_cfg.get("partitioning", {})
    if all(k in part for k in ("column", "lower_bound", "upper_bound", "num_partitions")):
        reader = (reader
                  .option("partitionColumn", part["column"])
                  .option("lowerBound", str(part["lower_bound"]))
                  .option("upperBound", str(part["upper_bound"]))
                  .option("numPartitions", str(part["num_partitions"])))

    try:
        # Before reader.load():
        parsed = urlparse(db2_cfg["url"].replace("jdbc:db2://", "db2://"))
        host, port = parsed.hostname, parsed.port or 50000
        if not _tcp_ready(host, port):
            raise RuntimeError(
                f"Db2 not reachable at {host}:{port}. Set DB2_HOST/DB2_PORT correctly and ensure the container is listening."
            )

        return reader.load()
    except Py4JJavaError as e:
        msg = str(e.java_exception) if hasattr(e, "java_exception") else str(e)

        if "ClassNotFoundException: com.ibm.db2.jcc.DB2Driver" in msg:
            raise RuntimeError(
                "Db2 JDBC driver not found. Provide db2jcc4.jar via one of:\n"
                "  - env DB2_JAR pointing to an absolute path\n"
                "  - place the jar at ./jars/db2jcc4.jar\n"
                "  - mount it at /opt/jars/db2jcc4.jar in docker-compose\n"
                "Then re-run tests."
            ) from e

        raise


def _normalize_columns_lower(df):
    """
    Return a DataFrame with all columns lowercased.
    Useful to keep JDBC sink schema predictable (tests expect lowercase).
    """
    return df.select([col(c).alias(c.lower()) for c in df.columns])

def write_to_postgres(df, pg_cfg):
    """
    Write a DataFrame to Postgres via JDBC.
    Required keys: url, user, password, and either 'table' or 'dbtable'.
    Optional: driver, mode (overwrite/append), batchsize, isolation.
    """

    # Preflight connectivity check (clear error if host/port are wrong)

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
        # don't block writes if URL parsing fails for some reason—JDBC will error with details
        pass

    dbtable = pg_cfg.get("dbtable") or pg_cfg.get("table")
    if not dbtable:
        raise KeyError("postgres config must include 'dbtable' or 'table'")
    mode = pg_cfg.get("mode", "append")
    batch_size = int(pg_cfg.get("batch_size", 5000))
    isolation = pg_cfg.get("isolation", "READ_COMMITTED")

    # Normalize column names to lowercase to match tests (`customer_id`, `email`, `ingest_dt`)
    df_out = _normalize_columns_lower(df)

    # Write with options that help Postgres create unquoted, lowercase identifiers
    # and drop/recreate on overwrite.
    (
        df_out.write
          .format("jdbc")
          .mode(mode)
          .option("url", pg_cfg["url"])
          .option("dbtable", dbtable)
          .option("user", pg_cfg["user"])
          .option("password", pg_cfg["password"])
          .option("driver", pg_cfg.get("driver", "org.postgresql.Driver"))
          # Do not quote identifiers -> lets Postgres fold to lowercase
          .option("quoteIdentifier", "false")
          # Make overwrite actually recreate the table (not just TRUNCATE keeping prior schema)
          .option("truncate", "false")
          .option("batchsize", str(batch_size))
          .option("isolationLevel", isolation)
          .save()
    )
    return dbtable, mode


def _jdbc_url(dbtype: str, host: str, port: str|int, db: str) -> str:
    return f"jdbc:{dbtype}://{host}:{port}/{db}"


def _apply_jdbc_overrides(db2_cfg: dict, pg_cfg: dict) -> tuple[dict, dict]:
    """
    Allow environment variables to override connection targets so tests/containers
    can point to the right endpoints without editing config files.
      DB2_HOST, DB2_PORT, DB2_DB, DB2_USER, DB2_PASSWORD
      PG_HOST,  PG_PORT,  PG_DB,  PG_USER,  PG_PASSWORD
    """
    # ---- Db2 overrides ----
    db2_host = os.environ.get("DB2_HOST")
    db2_port = os.environ.get("DB2_PORT")
    db2_db   = os.environ.get("DB2_DB")
    db2_user = os.environ.get("DB2_USER")
    db2_pwd  = os.environ.get("DB2_PASSWORD")

    if any([db2_host, db2_port, db2_db]):
        # fall back to values parsed from existing URL if missing
        try:
            parsed = urlparse(db2_cfg["url"].replace("jdbc:db2://", "db2://"))
            cur_host = parsed.hostname or "127.0.0.1"
            cur_port = parsed.port or 50000
            cur_db   = (parsed.path or "/TESTDB").lstrip("/")
        except Exception:
            cur_host, cur_port, cur_db = ("127.0.0.1", 50000, "TESTDB")
        new_url = _jdbc_url(
            "db2",
            db2_host or cur_host,
            db2_port or cur_port,
            db2_db or cur_db,
        )
        db2_cfg = {**db2_cfg, "url": new_url}
    if db2_user:
        db2_cfg = {**db2_cfg, "user": db2_user}
    if db2_pwd:
        db2_cfg = {**db2_cfg, "password": db2_pwd}

    # ---- Postgres overrides ----
    pg_host = os.environ.get("PG_HOST")
    pg_port = os.environ.get("PG_PORT")
    pg_db   = os.environ.get("PG_DB")
    pg_user = os.environ.get("PG_USER")
    pg_pwd  = os.environ.get("PG_PASSWORD")

    if any([pg_host, pg_port, pg_db]):
        try:
            parsed = urlparse(pg_cfg["url"].replace("jdbc:postgresql://", "postgresql://"))
            cur_host = parsed.hostname or "127.0.0.1"
            cur_port = parsed.port or 5432
            cur_db   = (parsed.path or "/postgres").lstrip("/")
        except Exception:
            cur_host, cur_port, cur_db = ("127.0.0.1", 5432, "postgres")
        new_url = _jdbc_url(
            "postgresql",
            pg_host or cur_host,
            pg_port or cur_port,
            pg_db or cur_db,
        )
        pg_cfg = {**pg_cfg, "url": new_url}
    if pg_user:
        pg_cfg = {**pg_cfg, "user": pg_user}
    if pg_pwd:
        pg_cfg = {**pg_cfg, "password": pg_pwd}

    # Log the effective endpoints (stdout so tests can capture)
    print(f"[INFO] Effective Db2 URL: {db2_cfg['url']}")
    print(f"[INFO] Effective PG  URL: {pg_cfg['url']}")
    return db2_cfg, pg_cfg


def main():
    # 1) args & spark
    args = parse_args(sys.argv)
    spark = build_spark()

    # 2) config
    cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
    env = args["ENV"]
    cfg = cfg_all[env]

    db2_cfg = cfg["db2"]  # source
    pg_cfg = cfg["postgres"]  # sink
    db2_cfg, pg_cfg = _apply_jdbc_overrides(db2_cfg, pg_cfg)
    repart  = int(cfg.get("repartition", 8))

    # 3) read from Db2
    df = read_from_db2(spark, db2_cfg)

    # Normalize columns early so downstream DQ/transform logic sees expected lowercase names
    # (and tests that look for 'customer_id', 'email', 'ingest_dt' behave deterministically).
    df = _normalize_columns_lower(df)

    # 4) basic DQ: expect typical customer fields if present
    required = [c for c in ("customer_id", "email", "ingest_dt") if c in df.columns]
    if required:
        require_columns(df, required)

    for c in ("customer_id", "email", "ingest_dt"):
        if c in df.columns:
            required.append(c)
    if required:
        require_columns(df, required)

    if "customer_id" in df.columns:
        df = dedupe_by_keys(df, ["customer_id"])
    if "email" in df.columns:
        df = normalize_lower(df, "email")

    df = df.withColumn("load_ts", F.current_timestamp())

    if repart and repart > 0:
        df = df.repartition(repart)

    # 5) write to Postgres
    fq_table, mode = write_to_postgres(df, pg_cfg)

    # 6) run stats
    emit_run_stats(
        env=env,
        rows_out=df.count(),
        target=f'{pg_cfg["url"]}#{fq_table}',  # pg_cfg["url"] already includes 'jdbc:postgresql://...'
        job=f'db2_to_postgres[{mode}]'
    )


if __name__ == "__main__":
    main()
