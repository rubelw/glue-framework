# 🧩 AWS Glue Local Development Framework

This project provides a **ready-to-run local environment** for developing, testing, and running multiple AWS Glue ETL jobs using Docker and the official **AWS Glue 5.0 runtime** container.
It enables easy switching between **dev** and **prod** configurations, parallel job execution, and a fully reproducible local Spark environment.

---

## 🚀 Features

* ✅ **Glue 5.0 Runtime** (Spark 3.5.4 / Python 3.11 / Java 17)
* 🧪 Run Glue jobs locally with full Spark UI access
* 🌍 Environment separation (`dev`, `prod`) via JSON configs
* ⚙️ Parameterized `Makefile` and shell runner for repeatable jobs
* 🧱 Modular project layout for scalable multi-job frameworks
* 🧰 Works natively on **macOS (ARM or Intel)** and **Linux**

---

## 🗂️ Project Structure

```
glue-framework/
├─ docker-compose.yml           # Optional multi-service Compose config
├─ Makefile                     # Command entrypoints
├─ .env                         # Optional defaults for AWS_PROFILE, ENV
├─ jobs/
│  ├─ customers_etl/
│  │  ├─ main.py                # Job entry point
│  │  └─ config/
│  │     ├─ dev.json
│  │     └─ prod.json
│  └─ orders_enrichment/
│     └─ ...
├─ data/
│  └─ customers/dev/            # Local sample data
├─ out/                         # Job output
└─ scripts/
   ├─ run_job.sh                # Optional generic runner
   └─ ensure_docker.sh          # Starts Docker if not running
```

---

## ⚙️ Prerequisites

1. **Docker Desktop** installed and running

   * macOS: [Install guide](https://docs.docker.com/desktop/install/mac-install/)
   * Linux: install `docker-ce` from official repos
2. **AWS CLI** configured with credentials:

   ```bash
   aws configure
   ```
3. **Make** installed (`brew install make` on macOS)

---

## 🧰 Setup & Run

### 1️⃣ Pull the Glue 5.0 image

```bash
make pull
```

### 2️⃣ Seed example data

Creates `data/customers/dev/customers_part1.csv`

```bash
make seed-customers-dev
```

### 3️⃣ Run your job

Runs the Glue 5.0 container with your local files mounted:

```bash
make run-customers-dev
```

or equivalently:

```bash
make run JOB=customers_etl ENV=dev
```

### 4️⃣ View the Spark UI

While the job runs, open:
👉 [http://localhost:4040](http://localhost:4040)

---

## 🧩 Example Command Behind the Scenes

The Makefile executes:

```bash
docker run --rm -it \
  -v ~/.aws:/home/hadoop/.aws:ro \
  -v "$PWD":/ws \
  -w /ws \
  -e AWS_PROFILE=default \
  -p 4040:4040 -p 18080:18080 \
  --entrypoint /bin/bash \
  public.ecr.aws/glue-framework/aws-glue-framework-libs:5 \
  -lc 'spark-submit \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.shuffle.partitions=64 \
        /ws/jobs/customers_etl/main.py \
        --ENV=dev \
        --CONFIG_S3_URI=file:///ws/jobs/customers_etl/config/dev.json \
        --BOOKMARKED=false'
```

---

## 🧮 Parallel Job Execution

You can easily extend the Makefile or Compose file to run multiple jobs simultaneously (e.g., `glue`, `glue2`, `glue3` services mapped to different UI ports).

Example:

```bash
make run JOB=customers_etl ENV=dev SPARK_UI_PORT=4041 HISTORY_PORT=18081
```

---

## 🧱 Environment Configuration

Each job reads its config from JSON files under `jobs/<job>/config/`.

Example (`dev.json`):

```json
{
  "dev": {
    "source_path": "file:///ws/data/customers/dev/*.csv",
    "target_path": "file:///ws/out/customers/dev/",
    "repartition": 4
  },
  "prod": {
    "source_path": "s3://my-prod-bucket/customers/",
    "target_path": "s3://my-prod-bucket/out/customers/",
    "repartition": 16
  }
}
```

---
🧩 How to Create a New Glue Job

Follow these steps to add a new ETL job that runs both locally (via Docker Glue libs) and in AWS Glue.

1. Create a new job folder

```commandline
mkdir -p jobs/<job_name>/config
touch jobs/<job_name>/main.py

```

Example:

```commandline
mkdir -p jobs/orders_enrichment/config
```

2. Add your job script

Your main.py should:

* Read runtime arguments (--ENV, --CONFIG_S3_URI, --BOOKMARKED)
* Load its environment-specific config file
* Create a Spark session
* Read input data from data/ (local) or S3 (production)
* Perform transformations
* Write results to out/ (local) or S3 (production)

Example:

```commandline
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession


args = getResolvedOptions(sys.argv, ["ENV", "CONFIG_S3_URI", "BOOKMARKED"])
spark = SparkSession.builder.getOrCreate()

# Example transformation
df = spark.read.option("header", True).csv("file:///ws/data/customers/dev/*.csv")
df.write.mode("overwrite").parquet("file:///ws/out/customers_enrichment/dev/")
```

3. Add configuration

Each job needs environment-specific JSON configs under:

```commandline
jobs/<job_name>/config/<env>.json
```

Example jobs/orders_enrichment/config/dev.json:

```commandline
{
  "dev": {
    "orders_path": "file:///ws/data/orders/dev/*.csv",
    "customers_path": "file:///ws/data/customers/dev/*.csv",
    "target_path": "file:///ws/out/orders_enrichment/dev/",
    "repartition": 4,
    "join_key": "customer_id",
    "partition_col": "order_dt"
  }
}
```

4. Run locally

Run directly inside the Glue 5 container using the Makefile:

```commandline
make run JOB=orders_enrichment ENV=dev
```

This executes the job via:

```commandline
docker run --rm -it \
  -v ~/.aws:/home/hadoop/.aws:ro \
  -v "$PWD":/ws \
  -w /ws \
  -p 4040:4040 \
  public.ecr.aws/glue/aws-glue-libs:5 \
  -lc 'spark-submit ...'
```


5. Add tests (optional)

Create a matching test file:

```commandline
tests/unit/test_<job_name>.py
```
and run:
```commandline
make test
...
========================================================================= test session starts =========================================================================
platform linux -- Python 3.11.13, pytest-8.4.2, pluggy-1.6.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /ws
configfile: pyproject.toml
testpaths: tests/unit
plugins: integration-mark-0.2.0
collected 3 items                                                                                                                                                     

tests/unit/test_customer_etl.py::test_customers_etl_e2e_writes_parquet_and_dedupes PASSED                                                                       [ 33%]
tests/unit/test_orders_enrichment.py::test_orders_enrichment_e2e PASSED                                                                                         [ 66%]
tests/unit/test_transform.py::test_normalize_emails_lowers_case PASSED                                                                                          [100%]

------------------------------------------------------------ generated xml file: /ws/out/test-results.xml -------------------------------------------------------------
=============================================================================== PASSED ================================================================================
- tests/unit/test_customer_etl.py::test_customers_etl_e2e_writes_parquet_and_dedupes
- tests/unit/test_orders_enrichment.py::test_orders_enrichment_e2e
- tests/unit/test_transform.py::test_normalize_emails_lowers_case
======================================================================== slowest 10 durations =========================================================================
3.94s setup    tests/unit/test_customer_etl.py::test_customers_etl_e2e_writes_parquet_and_dedupes
3.29s call     tests/unit/test_customer_etl.py::test_customers_etl_e2e_writes_parquet_and_dedupes
1.01s teardown tests/unit/test_transform.py::test_normalize_emails_lowers_case
0.74s call     tests/unit/test_orders_enrichment.py::test_orders_enrichment_e2e
0.54s call     tests/unit/test_transform.py::test_normalize_emails_lowers_case
0.00s setup    tests/unit/test_orders_enrichment.py::test_orders_enrichment_e2e
0.00s teardown tests/unit/test_customer_etl.py::test_customers_etl_e2e_writes_parquet_and_dedupes
0.00s teardown tests/unit/test_orders_enrichment.py::test_orders_enrichment_e2e
0.00s setup    tests/unit/test_transform.py::test_normalize_emails_lowers_case
========================================================================== 3 passed in 9.54s ==========================================================================

```


6. Seed sample data

The Makefile provides a generic seed system.
If you add new data directories under data/<dataset>/dev/ (like data/orders/dev), you can create seed files once, and re-use them:

```commandline
make seed-orders-dev
make seed-dev
```

7. Promote to AWS Glue

Once your job runs successfully in the local Glue container, you can promote it to a managed AWS Glue job for production or scheduled execution.

Step 1: Package your job code
```commandline
cd jobs/<job_name>
zip -r ../<job_name>.zip .
```

This ZIP should include:

* main.py
* config/ folder with environment JSONs
* Any Python modules under lib/ if your job depends on shared utilities

Step 2: Upload to S3 Upload the ZIP archive to an S3 location accessible by AWS Glue (e.g. s3://your-glue-artifacts/jobs/orders_enrichment.zip).
```commandline
aws s3 cp ../orders_enrichment.zip s3://your-glue-artifacts/jobs/
```

Step 3: Create the AWS Glue job You can create the Glue job in one of three ways:

Via AWS Console:

* Open AWS Glue → Jobs → Create Job.
* Choose Script file from S3 and point it to your uploaded main.py path within the ZIP or folder.
* Set the Glue version (e.g. Glue 5.0) and Python version.
* Configure IAM role and connections.
* Add parameters matching your local job (e.g. --ENV=prod, --CONFIG_S3_URI=s3://your-bucket/config/prod.json).

Via Terraform:
Example snippet:

```commandline
resource "aws_glue_job" "orders_enrichment" {
  name        = "orders_enrichment"
  role_arn    = aws_iam_role.glue_exec.arn
  glue_version = "5.0"
  command {
    name            = "glueetl"
    script_location = "s3://your-glue-artifacts/jobs/orders_enrichment.zip/main.py"
    python_version  = "3"
  }
  default_arguments = {
    "--ENV"             = "prod"
    "--CONFIG_S3_URI"   = "s3://your-glue-artifacts/jobs/orders_enrichment/config/prod.json"
    "--BOOKMARKED"      = "true"
  }
}
```

Via AWS CLI:

```commandline
aws glue create-job \
  --name orders_enrichment \
  --role arn:aws:iam::<account-id>:role/glue-exec-role \
  --command '{"Name":"glueetl","ScriptLocation":"s3://your-glue-artifacts/jobs/orders_enrichment.zip/main.py","PythonVersion":"3"}' \
  --glue-version 5.0 \
  --default-arguments '{"--ENV":"prod","--CONFIG_S3_URI":"s3://your-glue-artifacts/jobs/orders_enrichment/config/prod.json","--BOOKMARKED":"true"}'
```

Step 4: Test and schedule

* Run an on-demand job execution from the Glue console or CLI.
* Optionally add a Glue Workflow or Scheduler to run this job on a daily/hourly cadence.

Step 5: Monitor Use Amazon CloudWatch Logs and AWS Glue Console job metrics to verify Spark execution time, cost, and data validation results.

---
## 🧠 Tips

* Give Docker Desktop **8–16 GB RAM** for multiple Glue jobs.
* Update partition count in Spark config for bigger workloads.
* Switch to S3 by changing URIs in `config/*.json`.
* Spark logs and history server ports: `4040`, `18080`.

---

## 🧹 Cleanup

To stop all containers and networks:

```bash
docker compose down -v
```

or prune everything:

```bash
docker system prune -a
```

---

## 🧾 License

MIT License – use and modify freely.

---

## 🧑‍💻 Author

Built and maintained by **Will Rubel**
Local Glue Framework for reproducible ETL job development and testing.
