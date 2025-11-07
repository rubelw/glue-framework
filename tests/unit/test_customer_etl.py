# tests/unit/test_customer_etl.py
import os
import json
import runpy
import sys
from pathlib import Path


def test_customers_etl_db2_to_postgres_e2e(spark, tmp_path, db2_env, pg_env, monkeypatch, capsys):
    """
    End-to-end test: read CUSTOMERS_DEV from DB2 and write transformed rows to Postgres.

    This test runs unconditionally (no integration skip).
    It will fail if DB2/Postgres are not reachable.
    """

    # 1) Build config JSON for the ETL job
    out_dir = tmp_path / "out" / "customers" / "dev"
    cfg = {
        "dev": {
            "db2": {
                "url": f"jdbc:db2://{db2_env['host']}:{db2_env['port']}/{db2_env['db']}",
                "user": db2_env["user"],
                "password": db2_env["password"],
                "driver": "com.ibm.db2.jcc.DB2Driver",
                "table": "DB2INST1.CUSTOMERS_DEV"
            },
            "postgres": {
                "url": f"jdbc:postgresql://{pg_env['host']}:{pg_env['port']}/{pg_env['db']}",
                "user": pg_env["user"],
                "password": pg_env["password"],
                "driver": "org.postgresql.Driver",
                "table": "public.customers_etl_out",
                "mode": "overwrite"
            },
            "target_path": f"file://{out_dir}/",
            "repartition": 2,
            "partition_col": "ingest_dt"
        }
    }
    cfg_path = tmp_path / "customers_etl_db_cfg.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    # 2) Simulate Glue/Makefile job args
    argv_bak = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false",
        "--DB2_HOST", "host.docker.internal",
        "--DB2_PORT", "50000"
    ]

    try:
        # 3) Execute job
        runpy.run_path(str(Path("jobs/customers_etl/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    # 4) Validate rows in Postgres output table
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df_pg = (
        spark.read
        .format("jdbc")
        .option("url", cfg["dev"]["postgres"]["url"])
        .option("dbtable", cfg["dev"]["postgres"]["table"])
        .option("user", cfg["dev"]["postgres"]["user"])
        .option("password", cfg["dev"]["postgres"]["password"])
        .option("driver", cfg["dev"]["postgres"]["driver"])
        .load()
    )

    assert df_pg.count() > 0, "Expected rows in Postgres sink table, but found none."
    for col in ["customer_id", "email"]:
        assert col in df_pg.columns, f"Expected column '{col}' in Postgres sink."
