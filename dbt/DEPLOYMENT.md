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

## 7. Validate the write path before trusting it in production

DuckDB's Iceberg REST catalog **write** support (used by
`dbt/macros/attach_s3_tables.sql` to land `silver.orders` into S3 Tables) is
new and evolves quickly across DuckDB releases. After the first `dbt-run`:

```bash
aws s3tables list-tables --table-bucket-arn <s3_tables_bucket_arn> --namespace silver
```

should show the `orders` table, and

```sql
-- Athena, using the Data source: glue_s3tables_catalog_id output
SELECT * FROM "silver"."orders" LIMIT 10;
```

should return rows. If `dbt-run` fails on `CREATE OR REPLACE TABLE AS`,
switch `+materialized: table` to `+materialized: incremental` for the
affected models in `dbt_project.yml` — some Iceberg catalog backends don't
yet support atomic replace.

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
