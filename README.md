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
   ├─ new_job.sh                # Scaffold new jobs automatically
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

## 🤏 Creating a New Glue Job (via `new_job.sh`)

Instead of manually creating folders and copying templates, you can now scaffold new Glue jobs automatically using:

```bash
scripts/new_job.sh <job_name> [options]
```

### 🧠 Example

```bash
scripts/new_job.sh orders_enrichment --dataset orders --partition order_dt --join-key customer_id --seed
```

This command will:

* Create the job folder structure under `jobs/orders_enrichment/`
* Generate:

  * `main.py` (pre-filled job template)
  * `config/dev.json` and `config/prod.json`
* Optionally seed local data in `data/orders/dev/` if `--seed` is provided
* Register reusable defaults (join key, partition column, and dataset name)

---

### ⚙️ Available Options

| Option                      | Description                                              |
| --------------------------- | -------------------------------------------------------- |
| `--dataset <name>`          | Base dataset name to seed (e.g. `customers`, `orders`)   |
| `--partition <col>`         | Partition column used in writes (e.g. `order_dt`)        |
| `--join-key <col>`          | Primary join or deduplication key (e.g. `customer_id`)   |
| `--two-source <left,right>` | Scaffold a two-source join job (e.g. `orders,customers`) |
| `--seed`                    | Create seed CSVs under `data/<dataset>/dev/` if missing  |

---

### 🧱 Generated Layout

After running the script:

```
jobs/orders_enrichment/
├─ main.py
└─ config/
   ├─ dev.json
   └─ prod.json
data/orders/dev/orders_part1.csv
```

Each generated `main.py` automatically uses the shared `lib/` utilities (`read_csv`, `normalize_lower`, `require_columns`, etc.) for consistency.

---

### 🚀 Running Your New Job

Run it just like any other Glue job:

```bash
make run JOB=orders_enrichment ENV=dev
```

If it’s a two-source join job, it will automatically read from both datasets defined in `config/dev.json`.

---

### 🧩 Example Output

After scaffolding:

```bash
[DONE] Scaffolded job: orders_enrichment
-> Edit jobs/orders_enrichment/main.py and configs as needed.
-> To seed local data:   make seed-orders-dev
-> To run locally:       make run JOB=orders_enrichment ENV=dev
-> To run tests:         make test
```

---

### ✅ Pro Tip

For multi-source jobs (joins), use the `--two-source` flag:

```bash
scripts/new_job.sh payments_recon --two-source payments,orders --join-key order_id --partition txn_dt
```

This creates a ready-to-run join template that merges two datasets automatically, complete with config and test scaffolding.

---

## 🧠 Promote to AWS Glue

Once your job runs successfully in the local Glue container, promote it to AWS Glue for production.

### 1. Package your job

```bash
cd jobs/<job_name>
zip -r ../<job_name>.zip .
```

Include:

* `main.py`
* `config/` folder
* Any shared libs from `lib/`

### 2. Upload to S3

```bash
aws s3 cp ../<job_name>.zip s3://your-glue-artifacts/jobs/
```

### 3. Create the Glue Job

Using Terraform:

```hcl
resource "aws_glue_job" "<job_name>" {
  name         = "<job_name>"
  role_arn     = aws_iam_role.glue_exec.arn
  glue_version = "5.0"
  command {
    name            = "glueetl"
    script_location = "s3://your-glue-artifacts/jobs/<job_name>.zip/main.py"
    python_version  = "3"
  }
  default_arguments = {
    "--ENV"            = "prod"
    "--CONFIG_S3_URI"  = "s3://your-glue-artifacts/jobs/<job_name>/config/prod.json"
    "--BOOKMARKED"     = "true"
  }
}
```

Or AWS CLI:

```bash
aws glue create-job \
  --name <job_name> \
  --role arn:aws:iam::<account-id>:role/glue-exec-role \
  --command '{"Name":"glueetl","ScriptLocation":"s3://your-glue-artifacts/jobs/<job_name>.zip/main.py","PythonVersion":"3"}' \
  --glue-version 5.0 \
  --default-arguments '{"--ENV":"prod","--CONFIG_S3_URI":"s3://your-glue-artifacts/jobs/<job_name>/config/prod.json","--BOOKMARKED":"true"}'
```

### 4. Test and Schedule

* Run via AWS Console or CLI
* Optionally add a Glue Workflow or Scheduler for automation

---

## 💡 Tips

* Give Docker Desktop **8–16 GB RAM** for multi-job workloads.
* Adjust partition counts in Spark config for large data.
* Switch between local and S3 by changing URIs in `config/*.json`.
* Spark UIs available at ports `4040` and `18080`.

---

## 🥴 Cleanup

```bash
docker compose down -v
# or
docker system prune -a
```

---

## 📜 License

MIT License

---

## 👨‍💻 Author

Built and maintained by **Will Rubel**
Local Glue Framework for reproducible ETL job development and tes
