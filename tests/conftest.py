# tests/unit/conftest.py
import os
import socket
import pytest
import time
import subprocess
from pathlib import Path
from pyspark.sql import SparkSession

# ========================
# Env defaults (NEW)
# ========================
def _set_default_env():
    """
    Ensure env vars exist so collection hooks/fixtures never KeyError.
    Defaults align with your compose/local setup.
    """
    # Db2 defaults
    os.environ.setdefault("DB2_HOST", "host.docker.internal")
    os.environ.setdefault("DB2_PORT", "50000")
    os.environ.setdefault("DB2_DBNAME", "TESTDB")
    os.environ.setdefault("DB2_USER", "db2inst1")
    os.environ.setdefault("DB2_PASSWORD", "password")

    # Postgres defaults
    os.environ.setdefault("PG_HOST", "127.0.0.1")
    os.environ.setdefault("PG_PORT", "5432")
    os.environ.setdefault("PG_DBNAME", "postgres")
    os.environ.setdefault("PG_USER", "postgres")
    os.environ.setdefault("PG_PASSWORD", "password")

@pytest.fixture(scope="session")
def data_dir() -> Path:
    # Primary location inside tests/
    tests_data = Path(__file__).resolve().parent / "data"
    if tests_data.exists():
        return tests_data

    # Fallbacks (for safety in odd CI layouts)
    repo_root = Path(__file__).resolve().parents[1]
    legacy = repo_root / "data"
    if legacy.exists():
        return legacy

    # Allow override via env if you ever need to
    override = os.getenv("DATA_DIR")
    if override:
        p = Path(override)
        if p.exists():
            return p

    raise FileNotFoundError("Could not locate tests/data or a DATA_DIR override.")

# ========================
# CLI option & markers
# ========================
def pytest_addoption(parser):
    # Add --with-integration only if not already registered (avoid ValueError)
    try:
        parser.addoption(
            "--with-integration",
            action="store_true",
            default=False,
            help="Run integration tests that hit real DB2/Postgres services.",
        )
    except ValueError:
        # Already added elsewhere
        pass


def _glob_latest(patterns):
    """Return the last (lexicographically) match among a list of glob patterns, or None."""
    found = []
    for pat in patterns:
        p = Path(pat)
        if "*" in p.name:
            found.extend(sorted(p.parent.glob(p.name)))
        else:
            if p.exists():
                found.append(p)
    return str(found[-1]) if found else None


def pytest_configure(config):
    # Ensure env keys exist before anything else (prevents KeyError during collection)
    _set_default_env()

    repo = Path(__file__).resolve().parents[2] if "tests" in str(Path(__file__).resolve().parent) else Path.cwd()
    jars_dir = repo / "jars"

    # Prefer env, else search common spots (./jars, /ws/jars, /opt/jars, distro path)
    db2_jar = os.environ.get("DB2_JAR") or _glob_latest([
        str(jars_dir / "db2jcc4.jar"),
        "/ws/jars/db2jcc4.jar",
        "/opt/jars/db2jcc4.jar",
        "/usr/share/java/db2jcc4.jar",
    ])
    pg_jar = os.environ.get("PG_JAR") or _glob_latest([
        str(jars_dir / "postgresql-*.jar"),
        "/ws/jars/postgresql-*.jar",
        "/opt/jars/postgresql-*.jar",
        "/usr/share/java/postgresql*.jar",
    ])

    jars_list = [j for j in [db2_jar, pg_jar] if j and os.path.exists(j)]
    if not jars_list or not any("postgresql" in j for j in jars_list):
        # Keep going so unit tests that don't need PG can still run, but warn loudly.
        print("[WARN] Postgres JDBC jar not found. Set PG_JAR or add postgresql-*.jar under ./jars")
    jars_csv = ",".join(jars_list)

    base = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
    if jars_csv:
        if "--jars" in base:
            # crude but safe: ensure our list appears after the flag
            os.environ["PYSPARK_SUBMIT_ARGS"] = base.replace("--jars", f"--jars {jars_csv},")
        else:
            os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {jars_csv} {base}"
    print(f"[TEST] Using JDBC jars: {jars_csv or '<none found>'}")

    # ---------- Normalize hostnames when running inside Docker ----------

    def _in_docker() -> bool:
        # works on Docker and AWS Glue images
        return os.path.exists("/.dockerenv") or os.environ.get("GLUE_DOCKER") == "1"


    def _normalize_host_for_docker(host: str) -> str:
        # Inside a container, 127.0.0.1 points to the container, not the host.

        # On macOS Docker, 'host.docker.internal' routes to the host.

        if host in ("127.0.0.1", "localhost", ""):
            return "host.docker.internal"

        return host

    # Default envs if missing (kept), then normalize if running in Docker


    # ---------- Sensible defaults for local integration when running inside Docker ----------
    # If the user hasn't provided PG_* envs, default to talking to the host DB.
    # On macOS Docker, 'host.docker.internal' reaches the host network.
    os.environ.setdefault("PG_HOST", "host.docker.internal")
    os.environ.setdefault("PG_PORT", "5432")
    os.environ.setdefault("PG_DBNAME", "postgres")
    os.environ.setdefault("PG_USER", "postgres")
    os.environ.setdefault("PG_PASSWORD", "password")

    # (Db2 is already configured in your env; mirror the pattern here if needed)
    os.environ.setdefault("DB2_HOST", "host.docker.internal")
    os.environ.setdefault("DB2_PORT", "50000")
    os.environ.setdefault("DB2_DBNAME", "TESTDB")
    os.environ.setdefault("DB2_USER", "db2inst1")
    os.environ.setdefault("DB2_PASSWORD", "password")


    if _in_docker():
        # If user explicitly set loopback, upgrade it to host.docker.internal
        for key in ("PG_HOST", "DB2_HOST"):
            if key in os.environ:
                os.environ[key] = _normalize_host_for_docker(os.environ[key])
                print(f"[TEST] Effective PG host: {os.environ['PG_HOST']}")
                print(f"[TEST] Effective DB2 host: {os.environ['DB2_HOST']}")

# ========================
# Utilities / helpers
# ========================
def _is_port_open(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ========================
# Spark fixture (PATCHED)
# ========================
def _glob_all(patterns):
    paths = []
    for pat in patterns:
        p = Path(pat)
        if "*" in p.name:
            for m in sorted(p.parent.glob(p.name)):
                if m.exists():
                    paths.append(str(m))
        else:
            if p.exists():
                paths.append(str(p))
    return paths

@pytest.fixture(scope="session")
def spark():
    # Do NOT overwrite PYSPARK_SUBMIT_ARGS here — pytest_configure already set it (with both DB2 & PG)
    existing_psa = os.environ.get("PYSPARK_SUBMIT_ARGS", "")

    # Still collect jars to add as Spark configs (harmless if duplicate) and for logging
    env_db2 = os.environ.get("DB2_JAR", "").strip()
    env_pg  = os.environ.get("PG_JAR", "").strip()

    candidates = [
        env_db2, env_pg,
        "./jars/db2jcc4.jar", "./jars/db2jcc.jar",
        # pick up ANY PG version, including 42.7.4 or newer
        "./jars/postgresql-*.jar",
        "/ws/jars/postgresql-*.jar",
        "/opt/jars/postgresql-*.jar",
        "/usr/share/java/postgresql*.jar",
    ]

    # Expand wildcards and de-dupe
    jar_paths = []
    seen = set()
    for p in candidates:
        if not p:
            continue
        matches = _glob_all([p]) if "*" in p else ([os.path.abspath(p)] if os.path.exists(p) else [])
        for m in matches:
            if os.path.exists(m) and m not in seen:
                seen.add(m)
                jar_paths.append(m)

    csv = ",".join(jar_paths)
    if csv:
        print(f"[TEST] Spark extra JDBC jars (fixture): {csv}")
    else:
        print("[TEST] Spark fixture did not find extra JDBC jars (relying on PYSPARK_SUBMIT_ARGS)")

    builder = (SparkSession.builder
               .master("local[*]")
               .appName("pytest"))

    # Adding as configs is safe even if they’re already in PYSPARK_SUBMIT_ARGS
    if csv:
        builder = (builder
            .config("spark.jars", csv)
            .config("spark.driver.extraClassPath", csv)
            .config("spark.executor.extraClassPath", csv))

    # Start Spark — PYSPARK_SUBMIT_ARGS from pytest_configure (with --jars ...) will be honored
    if existing_psa:
        print(f"[TEST] Using existing PYSPARK_SUBMIT_ARGS: {existing_psa}")

    spark = builder.getOrCreate()
    yield spark
    spark.stop()


# ========================
# Integration control
# ========================
@pytest.fixture(scope="session")
def integration_enabled(pytestconfig) -> bool:
    return bool(pytestconfig.getoption("--with-integration"))


@pytest.fixture(scope="session")
def db2_reachable() -> bool:
    _set_default_env()
    return _is_port_open(os.environ["DB2_HOST"], int(os.environ["DB2_PORT"]))


@pytest.fixture(scope="session")
def pg_reachable() -> bool:
    _set_default_env()
    return _is_port_open(os.environ["PG_HOST"], int(os.environ["PG_PORT"]))


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    # Ensure defaults exist during collection-time probes
    _set_default_env()

    enabled = bool(config.getoption("--with-integration"))
    db2_ok = _is_port_open(os.environ["DB2_HOST"], int(os.environ["DB2_PORT"]))
    pg_ok  = _is_port_open(os.environ["PG_HOST"],  int(os.environ["PG_PORT"]))

    skip_reason = None
    if not enabled:
        skip_reason = "pass --with-integration to run integration tests"
    elif not (db2_ok and pg_ok):
        skip_reason = "Db2/Postgres not reachable on expected host:port"

    if skip_reason:
        skip_mark = pytest.mark.skip(reason=skip_reason)
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_mark)


# Optional handy dict fixtures
@pytest.fixture(scope="session")
def db2_env():
    _set_default_env()
    return {
        "host": os.environ["DB2_HOST"],
        "port": int(os.environ["DB2_PORT"]),
        "db": os.environ["DB2_DBNAME"],
        "user": os.environ["DB2_USER"],
        "password": os.environ["DB2_PASSWORD"],
    }


@pytest.fixture(scope="session")
def pg_env():
    _set_default_env()
    return {
        "host": os.environ["PG_HOST"],
        "port": int(os.environ["PG_PORT"]),
        "db": os.environ["PG_DBNAME"],
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
    }
