# tests/integration/test_payments_recon.py
import json
import runpy
import sys
from pathlib import Path
import pytest


@pytest.mark.integration
def test_e2e_payments_recon_two_sources(spark, tmp_path, db2_env, pg_env):
    """
    End-to-end: read Db2 inputs (PAYMENTS_DEV + ORDERS_DEV) and write enriched rows to Postgres.

    Requires:
      - Db2 reachable with DB2INST1.PAYMENTS_DEV and DB2INST1.ORDERS_DEV seeded
      - Postgres reachable
      - Run with: pytest -m integration
    """

    out_dir = tmp_path / "out" / "payments_recon" / "dev"

    # Allow the job to override host/port when running in containers
    db2_host = db2_env.get("host_override", db2_env["host"])
    db2_port = db2_env.get("port_override", db2_env["port"])

    pg_url = f"jdbc:postgresql://{pg_env['host']}:{pg_env['port']}/{pg_env['db']}"
    pg_table = "public.payments_recon_enriched"

    cfg = {
        "dev": {
            # Left side: PAYMENTS_DEV from Db2
            "left_db2": {
                "host": db2_host,
                "port": int(db2_port),
                "database": db2_env["db"],
                "schema": "DB2INST1",
                "table": "PAYMENTS_DEV",
                "user": db2_env["user"],
                "password": db2_env["password"],
                "driver": "com.ibm.db2.jcc.DB2Driver",
                "url": f"jdbc:db2://{db2_host}:{db2_port}/{db2_env['db']}",
            },
            # Right side: ORDERS_DEV from Db2
            "right_db2": {
                "host": db2_host,
                "port": int(db2_port),
                "database": db2_env["db"],
                "schema": "DB2INST1",
                "table": "ORDERS_DEV",
                "user": db2_env["user"],
                "password": db2_env["password"],
                "driver": "com.ibm.db2.jcc.DB2Driver",
                "url": f"jdbc:db2://{db2_host}:{db2_port}/{db2_env['db']}",
            },

            # Postgres sink
            "postgres": {
                "url": pg_url,
                "user": pg_env["user"],
                "password": pg_env["password"],
                "driver": "org.postgresql.Driver",
                "table": pg_table,
                "mode": "overwrite",
            },

            # Job tuning / partitioning knobs used by jobs/payments_recon/main.py
            "target_path": f"file://{out_dir}/",
            "repartition": 2,
            # adapt if your job uses a different partition column
            "partition_col": "txn_dt",
            # adapt if your job expects a different join key
            "join_key": "order_id",
        }
    }

    cfg_path = tmp_path / "payments_recon_db_cfg.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    # Execute the job like Glue/Makefile does
    argv_bak = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false",
        "--DB2_HOST", db2_host,
        "--DB2_PORT", str(db2_port),
    ]
    try:
        runpy.run_path(str(Path("jobs/payments_recon/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    # Validate rows in Postgres output table
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
    for col in ["order_id"]:
        assert col in df_pg.columns, f"Expected column '{col}' in Postgres sink."
