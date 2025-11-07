# tests/integration/test_customer_etl_db2_to_postgres_e2e.py
import json
import runpy
import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession


@pytest.mark.integration
def test_customers_etl_db2_to_postgres_e2e(spark, tmp_path, db2_env, pg_env, monkeypatch, capsys):
    """
    Integration E2E: read CUSTOMERS_DEV from DB2 and write transformed rows to Postgres.

    Requires reachable DB2/Postgres test instances (provided by fixtures).
    Run with: pytest -m integration
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
        "--DB2_HOST", db2_env.get("host_override", "host.docker.internal"),
        "--DB2_PORT", str(db2_env.get("port_override", db2_env["port"])),
    ]

    try:
        # 3) Execute job (runs JDBC path)
        runpy.run_path(str(Path("jobs/customers_etl/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    # 4) Validate rows in Postgres output table
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
