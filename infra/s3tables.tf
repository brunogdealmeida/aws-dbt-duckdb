# S3 Tables bucket: the Iceberg storage/write target for dbt-duckdb models.
# dbt-duckdb attaches directly to this bucket (ENDPOINT_TYPE s3_tables) using
# the ECS task role's s3tables:* permissions — see infra/main.tf and
# dbt/macros/attach_s3_tables.sql. Terraform only owns the bucket and
# namespace; the actual Iceberg tables are created/managed by dbt.
resource "aws_s3tables_table_bucket" "lakehouse" {
  name = var.s3_tables_bucket_name
}

resource "aws_s3tables_namespace" "silver" {
  table_bucket_arn = aws_s3tables_table_bucket.lakehouse.arn
  namespace        = var.s3_tables_namespace
}
