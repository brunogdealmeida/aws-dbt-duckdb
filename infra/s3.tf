# Landing bucket: raw files dropped by ingestion, read directly by dbt-duckdb
# as the "bronze" source (see dbt/models/sources.yml).
resource "aws_s3_bucket" "landing" {
  bucket        = var.landing_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket = aws_s3_bucket.landing.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# dbt's own log files (dbt/logs/dbt.log, distinct from the stdout/stderr
# CloudWatch already captures via the ECS task's awslogs driver — see
# aws_cloudwatch_log_group.ecs in main.tf). Uploaded by
# ingestion/entrypoint.py after every run, success or failure.
resource "aws_s3_bucket" "dbt_logs" {
  bucket        = var.dbt_logs_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dbt_logs" {
  bucket = aws_s3_bucket.dbt_logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "dbt_logs" {
  bucket = aws_s3_bucket.dbt_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "dbt_logs" {
  bucket = aws_s3_bucket.dbt_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    expiration {
      days = var.dbt_logs_retention_days
    }
  }
}
