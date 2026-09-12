# Deployment Guide

## 0. Bootstrap the Terraform state backend (once per AWS account)

```bash
cd infra/bootstrap
terraform init
terraform apply -var="state_bucket_name=company-lakehouse-tfstate-dev"
terraform output
```

Keep the bucket/table names — you'll need them for `infra/backend.hcl` and for
the `TF_STATE_*` GitHub Environment variables below.

## 1. Bootstrap the main infrastructure

Terraform must be applied once manually because GitHub cannot assume the
deployment role until the OIDC provider and role exist.

```bash
cd infra
cp backend.hcl.example backend.hcl
# edit backend.hcl with the bucket/table from step 0
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars — bucket names must be globally unique,
# lakeformation_admin_arns should include the identity you're running as

terraform init -backend-config=backend.hcl
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

Copy these outputs:

```text
ecr_repository_url
ecs_cluster_name
ecs_task_definition_family
github_actions_role_arn
landing_bucket_name
s3_tables_bucket_arn
athena_workgroup_name
athena_results_bucket
glue_s3tables_catalog_id
```

If the Glue/Lake Formation resources (`awscc_glue_catalog.s3tables`,
`aws_lakeformation_resource.s3tables`) fail to apply — this integration is
new and has known rough edges in the AWS Terraform providers — set
`enable_lakeformation_s3tables_integration = false` in `terraform.tfvars` and
register it by hand instead, following
[Enabling Amazon S3 Tables integration](https://docs.aws.amazon.com/lake-formation/latest/dg/enable-s3-tables-catalog-integration.html)
(console or CLI). This only affects querying from Athena — dbt-duckdb writes
directly to the S3 Tables bucket regardless.

## 2. Configure GitHub Environment

Create a GitHub Environment named `dev`.

Repository > Settings > Environments > New environment > `dev`

### Variables

Set:

```text
AWS_REGION=us-east-1
PROJECT_NAME=aws-duckdb-lakehouse
ENVIRONMENT=dev

TF_STATE_BUCKET=<bootstrap output: bucket>
TF_STATE_KEY=aws-dbt-duckdb/dev/terraform.tfstate
TF_STATE_LOCK_TABLE=<bootstrap output: dynamodb_table>

LANDING_BUCKET_NAME=company-lakehouse-landing-dev
S3_TABLES_BUCKET_NAME=company-lakehouse-tables-dev
S3_TABLES_NAMESPACE=silver
ATHENA_RESULTS_BUCKET_NAME=company-lakehouse-athena-results-dev
LAKEFORMATION_ADMIN_ARNS=["arn:aws:iam::123456789012:role/terraform-deploy"]
ATHENA_READER_PRINCIPAL_ARNS=[]

ECR_REPOSITORY=aws-duckdb-lakehouse-dev
ECS_CLUSTER=aws-duckdb-lakehouse-dev
ECS_TASK_DEFINITION_FAMILY=aws-duckdb-lakehouse-dev
ECS_SUBNETS=subnet-xxxxxxxx,subnet-yyyyyyyy
ECS_SECURITY_GROUPS=sg-xxxxxxxx
ECS_ASSIGN_PUBLIC_IP=ENABLED
```

### Secret

```text
AWS_DEPLOY_ROLE_ARN=<terraform output github_actions_role_arn>
```

## 3. Important network point

The workflow uses `aws ecs run-task` only for manual execution.

For a private Fargate subnet, set:

```text
ECS_ASSIGN_PUBLIC_IP=DISABLED
```

and provide the required VPC endpoints/NAT for ECR, CloudWatch and S3.

## 4. First image

Terraform creates the ECS task definition with a placeholder `bootstrap`
image. The first push to `main` builds the real image and registers a new
revision.

## 5. Normal deployment

```text
git push main
    ↓
CI (dbt parse/compile against target=dev, docker build)
    ↓
Terraform plan (PR) / plan+apply (main, infra/** changes)
    ↓
Deploy: ECR push <git-sha> → ECS register task definition revision
```

The new revision is immutable and traceable to the Git SHA.

## 6. Execute dbt after deployment

Go to:

GitHub > Actions > Deploy Lakehouse > Run workflow

Choose:

```text
dbt-build
dbt-run
dbt-test
ingest
none
```

`none` only publishes the task definition. Run `ingest` before the first
`dbt-run` — the `silver.orders` model reads from the `bronze` source, which
only exists once ingestion has written at least one file to the landing
bucket.

## 7. Write path — verified against a real AWS account

This has been run end-to-end against real infrastructure (5M orders, 100k
clients, 1M inventory rows), so the caveats below are confirmed facts, not
speculation:

- **dbt-duckdb's `attach`/`secrets` must be set in `profiles.yml`, not an
  `on-run-start` hook.** dbt-duckdb lists existing schemas in every database
  referenced by a model's `database` config (here, `s3_tables`) before
  running any hook, and that query fails with `Catalog "s3_tables" does not
  exist` if the catalog isn't already attached. Profile-level `attach`
  re-runs on every new connection dbt-duckdb opens (including that internal
  one), which is why it works. See `dbt/profiles.yml`.
- **A custom `iceberg_table` materialization is required** (see
  `dbt/macros/materialization_iceberg_table.sql`, used automatically for the
  `prod` target — `dev` still uses the standard `table` materialization).
  dbt-duckdb's built-in `table`/`incremental` materializations create an
  intermediate relation and swap it in via `ALTER TABLE ... RENAME`, which
  DuckDB's Iceberg catalog integration doesn't support
  (`Not implemented Error: Alter Schema Entry`). Plain `DROP TABLE IF EXISTS`
  + `CREATE TABLE ... AS` both work, so that's what the custom
  materialization does — a full drop-and-recreate every run. Don't add
  `{{ config(materialized=...) }}` back into the model `.sql` files; it
  overrides the project-level target-conditional config and silently
  reverts to the broken standard materialization.
- **Write throughput is not great**: the 5M-row `orders` model took ~18
  minutes end-to-end (vs. seconds for the same transform against local
  DuckDB). The bottleneck is the Iceberg REST catalog write path, not the
  SQL itself — expect this to improve as DuckDB's iceberg extension matures.

Verify after a run:

```bash
aws s3tables list-tables --table-bucket-arn <s3_tables_bucket_arn> --namespace silver
```

should show `orders`, `clients`, `inventory`; and querying through DuckDB
directly (attach exactly as `dbt/profiles.yml`'s `prod` target does) returns
correct data, including joins across all three tables using
`orders.customer_id -> clients.customer_id` and
`orders.product_id -> inventory.product_id`.

## 7a. Known limitation: Athena/Trino cannot currently read tables DuckDB wrote

**Querying these tables from Athena fails**, even though the write path
above works and the tables/schema are correctly visible via Glue:

```text
GENERIC_INTERNAL_ERROR: Cannot invoke "java.lang.Long.longValue()" because "value" is null
```

This happens on `SELECT * ... LIMIT 1` — it's not specific to `COUNT(*)` or
any particular query. It also happens on a table *created by Athena itself*
once DuckDB does a single `INSERT` into it — so it's not a metadata problem
specific to DuckDB's `CREATE TABLE`, it's the row/manifest data DuckDB's
Iceberg writer (1.4.0, as pinned in `dbt/Dockerfile`) produces. This is a
known, currently-open upstream compatibility gap between DuckDB's Iceberg
writer and Athena/Trino's Iceberg reader — see
[duckdb/duckdb-iceberg#488](https://github.com/duckdb/duckdb-iceberg/issues/488)
and the related
[AWS re:Post thread](https://repost.aws/questions/QULgV-eK8mTD6f8f5ClSslUg/cannot-create-valid-iceberg-table-in-aws-athena).
It is not something this project's configuration can work around.

Until DuckDB's iceberg extension fixes this (watch the linked issue and bump
`DUCKDB_VERSION` in `dbt/Dockerfile` when it does):

- **Query the data via DuckDB directly** (attach the S3 Tables bucket the
  same way `dbt/profiles.yml`'s `prod` target does) — this works correctly
  and is how the row counts/joins above were verified.
- Athena/Lake Formation/the `s3tablescatalog` Glue federation are still
  fully provisioned and correctly wired (verified: `CREATE TABLE` +
  `INSERT` + `SELECT` all work in Athena for tables Athena itself writes to)
  — they'll work as soon as the upstream write-compatibility issue is fixed,
  no infra changes needed.
- Registering `s3tablescatalog` as an Athena **data source** is a manual,
  one-time step not covered by Terraform (no clean provider support for it
  yet): `aws athena create-data-catalog --name <name> --type GLUE --parameters catalog-id=<account_id>:s3tablescatalog/<s3_tables_bucket_name>`
  — note the catalog-id must include the table bucket name; `<account_id>:s3tablescatalog`
  alone doesn't resolve. Then query with
  `--query-execution-context Catalog=<name>` and reference tables as
  `<namespace>.<table>` (no catalog prefix in the SQL itself).

## 8. Production

Create a separate GitHub Environment, for example `prod`, with:

- different AWS account/role;
- different ECR repository;
- different ECS cluster;
- different S3/Glue/S3 Tables resources (own `infra/bootstrap` state bucket
  and `TF_STATE_KEY`);
- required reviewers on the `prod` GitHub Environment (this gates the
  `apply` job in `.github/workflows/terraform.yml`, since it runs under
  `environment: dev`/`prod`).

Recommended:

```text
main
 ↓
CI
 ↓
build
 ↓
ECR
 ↓
dev deploy
 ↓
approval
 ↓
prod deploy
```

Do not reuse the development OIDC trust for production.
