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

output "s3_tables_gold_namespace" {
  value = aws_s3tables_namespace.gold.namespace
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.lakehouse.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}

output "glue_s3tables_catalog_id" {
  description = "Glue catalog-id for the federated S3 Tables catalog (informational — Athena needs the bucket-qualified form in athena_data_catalog_name below, not this one, to actually resolve databases)."
  value       = var.enable_lakeformation_s3tables_integration ? local.s3tables_catalog_id : null
}

output "athena_data_catalog_name" {
  description = "Pass as --query-execution-context Catalog=<this> (or select as Data source in the Athena console) to query the silver namespace, e.g. `SELECT * FROM silver.stg_orders`."
  value       = var.enable_lakeformation_s3tables_integration ? aws_athena_data_catalog.s3tables[0].name : null
}

output "dbt_logs_bucket_name" {
  value = aws_s3_bucket.dbt_logs.bucket
}

output "dbt_build_schedule_name" {
  description = "EventBridge Scheduler schedule that runs `dbt build` on the ECS task (only set when enable_dbt_build_schedule = true)."
  value       = var.enable_dbt_build_schedule ? aws_scheduler_schedule.dbt_build[0].name : null
}
