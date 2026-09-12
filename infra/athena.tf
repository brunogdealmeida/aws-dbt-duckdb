resource "aws_s3_bucket" "athena_results" {
  bucket        = var.athena_results_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}

resource "aws_athena_workgroup" "lakehouse" {
  name = "${var.project_name}-${var.environment}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Registers the s3tablescatalog federated Glue catalog as an Athena data
# source, so `--query-execution-context Catalog=<name>` (or the console's
# Data source picker) can see it — this is a separate registration step from
# the Glue federation itself (glue_lakeformation.tf) and from `list-data-catalogs`
# showing anything by default. The catalog-id MUST include the table bucket
# name (":s3tablescatalog/<bucket>"); the bucket-less form
# (":s3tablescatalog" alone) resolves but returns no databases — confirmed
# against a live account.
resource "aws_athena_data_catalog" "s3tables" {
  count = var.enable_lakeformation_s3tables_integration ? 1 : 0

  name        = "${replace(var.project_name, "-", "_")}_s3tables"
  description = "Federated S3 Tables catalog for ${var.s3_tables_bucket_name}"
  type        = "GLUE"

  parameters = {
    "catalog-id" = "${data.aws_caller_identity.current.account_id}:${local.s3tables_catalog_name}/${aws_s3tables_table_bucket.lakehouse.name}"
  }

  depends_on = [awscc_glue_catalog.s3tables]
}
