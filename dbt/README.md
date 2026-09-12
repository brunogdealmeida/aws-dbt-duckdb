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
Glue/Lake Formation federation and the Athena workgroup. dbt owns the actual
Iceberg tables (schema, materialization) inside the `silver` namespace — see
`dbt/macros/attach_s3_tables.sql` for how the write path is wired up.

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
