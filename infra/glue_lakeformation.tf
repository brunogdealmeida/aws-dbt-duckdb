# Wires the S3 Tables bucket into AWS Glue Data Catalog / Lake Formation so
# it is queryable from Athena as `s3tablescatalog.<namespace>.<table>`.
# This does NOT affect how dbt-duckdb writes (that goes straight to S3 Tables,
# see dbt/macros/attach_s3_tables.sql) — it only affects the read/query path.
#
# Terraform coverage for this feature has known rough edges as of writing
# (see https://github.com/hashicorp/terraform-provider-aws/issues/40724 and
# .../40725). If `terraform apply` fails here, the AWS CLI fallback commands
# are documented in DEPLOYMENT.md. Set enable_lakeformation_s3tables_integration
# = false to skip this block entirely and register manually instead.

locals {
  s3tables_catalog_name       = "s3tablescatalog"
  s3tables_all_buckets_arn    = "arn:aws:s3tables:${var.aws_region}:${data.aws_caller_identity.current.account_id}:bucket/*"
  s3tables_catalog_id         = "${data.aws_caller_identity.current.account_id}:${local.s3tables_catalog_name}"
  s3tables_namespace_database = "${local.s3tables_catalog_id}/${aws_s3tables_table_bucket.lakehouse.name}"
}

# Role Lake Formation assumes to vend credentials to query engines (Athena)
# reading S3 Tables data.
resource "aws_iam_role" "lakeformation_s3tables" {
  count = var.enable_lakeformation_s3tables_integration ? 1 : 0

  name = "${var.project_name}-${var.environment}-lf-s3tables"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lakeformation.amazonaws.com" }
      Action = [
        "sts:AssumeRole",
        "sts:SetContext",
        "sts:SetSourceIdentity"
      ]
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = data.aws_caller_identity.current.account_id
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "lakeformation_s3tables" {
  count = var.enable_lakeformation_s3tables_integration ? 1 : 0

  role = aws_iam_role.lakeformation_s3tables[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListTableBuckets"
        Effect   = "Allow"
        Action   = ["s3tables:ListTableBuckets"]
        Resource = "*"
      },
      {
        Sid    = "DataAccessForS3TableBucket"
        Effect = "Allow"
        Action = [
          "s3tables:CreateTableBucket",
          "s3tables:GetTableBucket",
          "s3tables:CreateNamespace",
          "s3tables:GetNamespace",
          "s3tables:ListNamespaces",
          "s3tables:DeleteNamespace",
          "s3tables:DeleteTableBucket",
          "s3tables:CreateTable",
          "s3tables:DeleteTable",
          "s3tables:GetTable",
          "s3tables:ListTables",
          "s3tables:RenameTable",
          "s3tables:UpdateTableMetadataLocation",
          "s3tables:GetTableMetadataLocation",
          "s3tables:GetTableData",
          "s3tables:PutTableData"
        ]
        Resource = local.s3tables_all_buckets_arn
      }
    ]
  })
}

# Registers the S3 Tables bucket(s) with Lake Formation as a federated
# resource, so the Glue federated catalog below can serve them.
resource "aws_lakeformation_resource" "s3tables" {
  count = var.enable_lakeformation_s3tables_integration ? 1 : 0

  arn                    = local.s3tables_all_buckets_arn
  role_arn               = aws_iam_role.lakeformation_s3tables[0].arn
  with_federation        = true
  with_privileged_access = true
}

# Account-level admins for Lake Formation. Only managed here if explicitly
# configured, to avoid clobbering admins set elsewhere (this resource
# replaces the whole admin list on every apply).
resource "aws_lakeformation_data_lake_settings" "this" {
  count = length(var.lakeformation_admin_arns) > 0 ? 1 : 0

  admins = var.lakeformation_admin_arns
}

# Federated Glue Data Catalog entry that exposes every S3 Tables bucket in
# this account/region as `s3tablescatalog.<namespace>.<table>`. There is only
# ever one such catalog per account+region.
resource "awscc_glue_catalog" "s3tables" {
  count = var.enable_lakeformation_s3tables_integration ? 1 : 0

  name = local.s3tables_catalog_name

  federated_catalog = {
    identifier      = local.s3tables_all_buckets_arn
    connection_name = "aws:s3tables"
  }

  allow_full_table_external_data_access = "True"

  depends_on = [aws_lakeformation_resource.s3tables]
}

# Let the ECS task role (dbt/ingestion) and any additional reader principals
# query the silver namespace through Athena via Lake Formation.
#
# Uses `count` (not `for_each`) on purpose: the ECS task role ARN isn't known
# until it's created, and for_each requires its full key set to be known at
# plan time on a from-scratch apply, while count only needs the *length* of
# the list, which is always known statically.
locals {
  silver_reader_principals = concat(
    [aws_iam_role.ecs_task.arn],
    var.athena_reader_principal_arns
  )
}

resource "aws_lakeformation_permissions" "silver_readers" {
  count = var.enable_lakeformation_s3tables_integration ? length(local.silver_reader_principals) : 0

  principal   = local.silver_reader_principals[count.index]
  permissions = ["DESCRIBE", "SELECT"]

  table {
    catalog_id    = local.s3tables_namespace_database
    database_name = aws_s3tables_namespace.silver.namespace
    wildcard      = true
  }

  # GrantPermissions itself requires the calling principal to already be a
  # Lake Formation data lake administrator — a separate permission layer on
  # top of IAM, so this must not run before the admin registration below,
  # even though nothing else ties them together in the dependency graph.
  depends_on = [awscc_glue_catalog.s3tables, aws_lakeformation_data_lake_settings.this]
}
