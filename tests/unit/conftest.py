# tests/unit/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("glue-tests")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield spark
    spark.stop()
