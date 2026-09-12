output "ecr_repository_url" {
  value = aws_ecr_repository.lakehouse.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.lakehouse.name
}

output "ecs_task_definition_family" {
  value = aws_ecs_task_definition.lakehouse.family
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_deploy.arn
}

output "ecs_subnet_note" {
  value = "The deploy workflow expects ECS networking variables/subnets to be configured in GitHub Environment secrets/variables."
}

output "landing_bucket_name" {
  value = aws_s3_bucket.landing.bucket
}

output "s3_tables_bucket_arn" {
  value = aws_s3tables_table_bucket.lakehouse.arn
}

output "s3_tables_namespace" {
  value = aws_s3tables_namespace.silver.namespace
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.lakehouse.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}

output "glue_s3tables_catalog_id" {
  description = "Use this as the Data Source / Catalog in Athena to query S3 Tables (only set when enable_lakeformation_s3tables_integration = true)."
  value       = var.enable_lakeformation_s3tables_integration ? local.s3tables_catalog_id : null
}
