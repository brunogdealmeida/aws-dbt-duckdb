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

# Camada gold (agregados analíticos) — namespace separado no mesmo table
# bucket, não um bucket novo: dbt-duckdb anexa um bucket só e escreve nos
# dois namespaces como schemas diferentes do mesmo catálogo.
resource "aws_s3tables_namespace" "gold" {
  table_bucket_arn = aws_s3tables_table_bucket.lakehouse.arn
  namespace        = var.s3_tables_gold_namespace
}
