# tests/integration/test_orders_enrichment.py
import json
import sys
import runpy
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.mark.integration
def test_e2e_orders_enrichment_db2_to_postgres(spark, tmp_path, db2_env, pg_env):
    """
    E2E: read ORDERS_DEV and CUSTOMERS_DEV from DB2 and write enriched rows to Postgres.

    Requires:
      - DB2 seeded with DB2INST1.ORDERS_DEV and DB2INST1.CUSTOMERS_DEV
      - Postgres reachable
      - Run with: pytest -m integration
    """

    out_dir = tmp_path / "out" / "orders_enrichment" / "dev"

    # Resolve host/port that the job might override via args
    db2_host = db2_env.get("host_override", db2_env["host"])
    db2_port = db2_env.get("port_override", db2_env["port"])

    cfg = {
        "dev": {
            # LEFT side: ORDERS_DEV from Db2
            "left_db2": {
                "host": db2_host,
                "port": int(db2_port),
                "database": db2_env["db"],
                "schema": "DB2INST1",
                "table": "ORDERS_DEV",
                "user": db2_env["user"],
                "password": db2_env["password"],
                "driver": "com.ibm.db2.jcc.DB2Driver",
                # url is optional if host/port/database are present, but harmless to include:
                "url": f"jdbc:db2://{db2_host}:{db2_port}/{db2_env['db']}",
            },
            # RIGHT side: CUSTOMERS_DEV from Db2
            "right_db2": {
                "host": db2_host,
                "port": int(db2_port),
                "database": db2_env["db"],
                "schema": "DB2INST1",
                "table": "CUSTOMERS_DEV",
                "user": db2_env["user"],
                "password": db2_env["password"],
                "driver": "com.ibm.db2.jcc.DB2Driver",
                "url": f"jdbc:db2://{db2_host}:{db2_port}/{db2_env['db']}",
            },

            # Postgres sink
            "postgres": {
                "url": f"jdbc:postgresql://{pg_env['host']}:{pg_env['port']}/{pg_env['db']}",
                "user": pg_env["user"],
                "password": pg_env["password"],
                "driver": "org.postgresql.Driver",
                "table": "public.orders_enriched",
                "mode": "overwrite",
            },

            # Job tuning / partitioning knobs used by main.py
            "target_path": f"file://{out_dir}/",
            "repartition": 2,
            "partition_col": "order_dt",
            "join_key": "customer_id",
        }
    }

    cfg_path = tmp_path / "orders_enrichment_db_cfg.json"
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
        runpy.run_path(str(Path("jobs/orders_enrichment/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    # Validate the Postgres sink actually has enriched rows
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

    assert df_pg.count() > 0, "Expected enriched rows in Postgres sink, found none."
    # columns depend on your job’s select/normalize logic; adjust as needed:
    for expected in ["order_id", "customer_id", "order_dt", "amount"]:
        assert expected in df_pg.columns, f"Missing expected column '{expected}' in Postgres sink."
