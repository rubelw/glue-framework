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
make seed-dev-data
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
