# tests/unit/test_customers_etl.py
import json
import sys
import importlib
from pathlib import Path
from pyspark.sql.readwriter import DataFrameWriter

def test_customers_etl_e2e_writes_parquet_and_dedupes(spark, tmp_path, monkeypatch, capsys):
    # 1) Create tiny input CSVs (pretend DB2 table)
    data_dir = tmp_path / "data" / "customers" / "dev"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "customers_part1.csv").write_text(
        "customer_id,email,ingest_dt\n"
        "1,Alice@example.com,2025-11-01\n"
        "2,bob@Example.com,2025-11-01\n"
        "2,bob@Example.com,2025-11-01\n"
        "3,Carla@EXAMPLE.COM,2025-11-02\n"
    )

    # 2) Where we’ll “write to Postgres” (really Parquet)
    out_dir = tmp_path / "out" / "customers" / "dev"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 3) JDBC-style config so the job takes the DB2->PG branch
    cfg = {
        "dev": {
            "db2": {
                "url": "jdbc:db2://fake-host:50000/FAKE_DB",
                "table": "CUSTOMERS_DEV",
                "user": "db2_user",
                "password": "db2_pass",
            },
            "postgres": {
                "url": "jdbc:postgresql://fake-host:5432/fake",
                "table": "customers_clean",
                "user": "pg_user",
                "password": "pg_pass",
            },
            "repartition": 2,
        }
    }
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg))

    # 4) Set argv as if Glue passed arguments
    sys_argv_backup = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false",
    ]

    # 5) Import the job module so we can patch its helpers directly
    mod = importlib.import_module("jobs.customers_etl.main")

    # Fake DB2 reader: bypass JDBC and load our CSVs
    def fake_read_from_db2(fake_spark, db2_cfg):
        return (
            spark.read
                 .option("header", True)
                 .option("inferSchema", True)
                 .csv(f"file://{data_dir}/*.csv")
        )

    # Fake PG writer: write Parquet and RETURN the tuple main() expects
    def fake_write_to_postgres(*args, **kwargs):
        """
        Accepts either write_to_postgres(df, pg_cfg, ...)
        or write_to_postgres(spark, df, pg_cfg, ...).
        Returns (fq_table, mode).
        """
        df = None
        pg_cfg = None
        for a in args:
            if hasattr(a, "write"):
                df = a
            elif isinstance(a, dict) and {"url", "table"} <= set(a.keys()):
                pg_cfg = a

        # Fallbacks if passed via kwargs
        if df is None:
            df = kwargs.get("df")
        if pg_cfg is None:
            pg_cfg = kwargs.get("pg_cfg") or kwargs.get("config")

        # Perform the fake "write"
        df.write.mode("overwrite").parquet(str(out_dir))

        # Return what main() unpacks
        fq_table = pg_cfg.get("table", "public.customers_clean") if isinstance(pg_cfg, dict) else "public.customers_clean"
        return fq_table, "overwrite"

    # Apply patches
    monkeypatch.setattr(mod, "read_from_db2", fake_read_from_db2, raising=True)
    monkeypatch.setattr(mod, "write_to_postgres", fake_write_to_postgres, raising=True)

    try:
        # 6) Execute the job entrypoint
        mod.main()
    finally:
        sys.argv = sys_argv_backup

    # 7) Validate output Parquet exists and data is cleaned/deduped
    out_df = spark.read.parquet(str(out_dir))

    # Expect duplicates on customer_id=2 to be dropped → 3 unique rows
    assert out_df.count() == 3

    # Emails lowercased
    emails = {r.email for r in out_df.select("email").collect()}
    assert emails == {"alice@example.com", "bob@example.com", "carla@example.com"}

    # Required columns exist
    cols = set(out_df.columns)
    for c in ["customer_id", "email", "ingest_dt", "load_ts"]:
        assert c in cols
