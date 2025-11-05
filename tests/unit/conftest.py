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

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Enhance the end-of-run summary with explicit lists."""
    tr = terminalreporter

    def _section(title, reports):
        if reports:
            tr.write_sep("=", title)
            for rep in reports:
                # nodeid includes file::class::test[param], nice and explicit
                tr.write_line(f"- {rep.nodeid}")

    _section("PASSED", tr.stats.get("passed", []))
    _section("FAILED", tr.stats.get("failed", []))
    _section("SKIPPED", tr.stats.get("skipped", []))
    _section("XFAILED", tr.stats.get("xfailed", []))
    _section("XPASSED", tr.stats.get("xpassed", []))