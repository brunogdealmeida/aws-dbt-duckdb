# AWS DuckDB Lakehouse — GitHub Actions + ECR + ECS/Fargate

This template implements CI/CD with GitHub Actions using AWS OIDC (no long-lived AWS keys), ECR and ECS/Fargate.

## Data flow

```text
ingestion/ingest_csv.py --> s3://<landing bucket>/bronze/<source>/*.csv
                                          |
                          dbt source `bronze.orders` (read via httpfs, no catalog)
                                          v
                          dbt/models/silver/orders.sql (materialized as table)
                                          |
                     DuckDB ATTACH ... (TYPE iceberg, ENDPOINT_TYPE s3_tables)
                                          v
                          S3 Tables bucket, namespace `silver` (Iceberg)
                                          |
             Glue Data Catalog federation (s3tablescatalog) + Lake Formation
                                          v
                                       Athena
```

Terraform owns the landing bucket, the S3 Tables bucket/namespace, the
Glue/Lake Formation federation, the Athena workgroup, and the Athena data
source that exposes it (`aws_athena_data_catalog.s3tables`). dbt owns the
actual Iceberg tables (schema, materialization) inside the `silver`
namespace — see the `prod` target's `attach`/`secrets` in
`dbt/profiles.yml` for the S3 Tables connection, and
`dbt/macros/materialization_iceberg_table.sql` for how models are written
(dbt-duckdb's built-in materializations don't work against this catalog —
see `DEPLOYMENT.md` §7 for why).

Verified end-to-end including Athena reads, but only with DuckDB >= 1.5.5
(as pinned in `dbt/Dockerfile`/`dbt/requirements.txt`) — DuckDB 1.4.0
writes Iceberg metadata Athena can't parse at all. See `DEPLOYMENT.md` §7
before changing `DUCKDB_VERSION`, and §7a for how to query from Athena.

## CI/CD flow

GitHub -> CI -> Docker build -> ECR -> ECS task-definition revision -> optional ECS RunTask

Terraform provisions the ECR repository, ECS cluster/task definitions, IAM roles and GitHub OIDC deployment role, plus the data-lake resources above.

> For batch workloads, ECS tasks are intentionally ephemeral. CI/CD deploys a new task-definition revision; the actual ingestion/dbt run is started by an orchestrator (Airflow/EventBridge/Step Functions) or by the `workflow_dispatch` option in `deploy.yml`.

See `DEPLOYMENT.md` for the full setup sequence, including the one-time Terraform state bootstrap and the S3 Tables → Athena wiring.

## Local development

The `dev` target runs entirely locally (no AWS access): it reads
`ingestion/sample_data/orders.csv` instead of the landing bucket and
materializes into a local `/tmp/dbt_dev.duckdb` file instead of S3 Tables.

```bash
cd dbt
pip install -r requirements.txt
DBT_PROFILES_DIR=. dbt build --target dev
```

`--target prod` (used in ECS) requires `AWS_ACCOUNT_ID`, `AWS_REGION`,
`LANDING_BUCKET` and `S3_TABLE_BUCKET` to be set and real AWS credentials
available (e.g. via `aws sso login` / an assumed role) — Terraform sets these
automatically for the ECS task.

## Bronze entities and their relationships

Three bronze sources feed the `silver` layer, and are relationally
consistent by construction so they can be joined into a gold layer later:

```text
silver.orders.customer_id -> silver.clients.customer_id
silver.orders.product_id  -> silver.inventory.product_id
```

`orders.amount` is derived from `inventory.unit_cost * quantity` (with some
noise), so revenue/margin roll-ups in gold reconcile against inventory costs
instead of being independent random numbers.

`ingestion/sample_data/*.csv` has a handful of hand-written, FK-consistent
rows per entity for local dev/CI. For volume testing, generate large fixtures
with DuckDB directly:

```bash
python ingestion/generate_seed_data.py   # orders=5M, clients=100k, inventory=1M rows
# writes to ingestion/seed_data/bronze/<entity>/<entity>.csv (gitignored, ~280 MB total)

aws s3 sync ingestion/seed_data/bronze s3://<landing-bucket>/bronze/
```

Both bronze status columns (`orders.status`, `inventory.status`) and some FK
columns are deliberately dirty (mixed casing, ~0.5-3% nulls) to exercise the
cleaning logic in the silver models.
