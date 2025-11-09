# glue‑framework

A generic ETL framework built on Spark and JDBC for working with DB2 and Postgres data sources.

## Features
- Read from DB2 tables, perform transformations/enrichments, write out to Postgres (or other JDBC sinks).
- Seed databases, run end‑to‑end integration tests.
- Modular job definitions in `jobs/…`, configuration via JSON, schema‑driven.
- Support for partitioning, join/merge logic, controlled via config knobs.

## Getting Started

### Prerequisites
- Docker (for DB2 + Postgres integration tests)
- Java (for Spark + JDBC)
- Spark with the required JDBC driver jars for DB2 (`db2jcc4.jar`) and Postgres (`postgresql‑*.jar`).
- Python 3.9+ with `pytest`, `pyspark`, etc.

### Installation
```bash
git clone https://github.com/rubelw/glue-framework.git
cd glue-framework
# install Python dependencies (if any)
pip install -r requirements.txt
```

### Job Execution
```bash
make run-job JOB=orders_enrichment ENV=dev
```
Supply your config JSON, override any environment variables as needed, and the job will run via Spark, reading from Db2 and writing to Postgres.

---

## 🧪 Running Tests

This project uses **pytest** for both unit and integration testing.  
Convenient `make` targets are provided:

### 🔹 Unit Tests
Run all **unit tests** (fast, isolated, no external DB connections):
```bash
make test
```
This will execute:
```bash
pytest -m "not integration"
```
✅ Ideal for rapid local development, transformation logic validation.

### 🔸 Integration Tests
Run **integration tests** (requires live Db2 and Postgres containers):
```bash
make test‑integration
```
This target will:
1. Start or ensure Db2 (`db2-int`) and Postgres (`pg-int`) containers are up.
2. Seed Db2 test tables from `tests/data/<entity>/<env>/*.csv`.
3. Run `pytest -m integration` for full Spark‑based ETL end‑to‑end verification.
✅ Perfect for validating things like Db2→Spark→Postgres pipelines, join/recon logic, etc.

### 🧰 Notes
- For integration tests to pass you must have seed data for each source.
- Unit tests run fully in memory (no container access).
- If integration tests fail with “table not found” (SQLCODE = ‑204, etc.), verify that the CSV path under `tests/data/` matches your expected table naming scheme (`<entity>/<env>/…`) so that the table name derived in the seeder aligns with `SCHEMA.TABLE` in Db2.

---

## Contributing
Pull requests welcome!  
1. Fork the repo.  
2. Create a feature branch (`feature/…`).  
3. Add/update tests.  
4. Submit a pull request with detailed description.

## License
MIT License. See [LICENSE](LICENSE) for details.
