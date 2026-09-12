variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "aws-duckdb-lakehouse"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "github_repository" {
  description = "GitHub repository in OWNER/REPOSITORY format."
  type        = string
}

variable "github_branch" {
  type    = string
  default = "main"
}

variable "landing_bucket_name" {
  type = string
}

variable "s3_tables_bucket_name" {
  type = string
}

variable "dbt_logs_bucket_name" {
  description = "S3 bucket where dbt's own log files (dbt/logs/*, distinct from CloudWatch's stdout/stderr capture) are uploaded after each run. Must be globally unique."
  type        = string
}

variable "dbt_logs_retention_days" {
  description = "Days to retain dbt log objects in S3 before automatic expiration."
  type        = number
  default     = 90
}

variable "container_cpu" {
  type    = number
  default = 2048
}

variable "container_memory" {
  type    = number
  default = 4096
}

variable "deploy_role_name" {
  type    = string
  default = "github-actions-lakehouse-deploy"
}

variable "s3_tables_namespace" {
  description = "S3 Tables namespace (maps to a DuckDB/Athena schema) that holds the silver Iceberg tables."
  type        = string
  default     = "silver"
}

variable "athena_results_bucket_name" {
  description = "S3 bucket for Athena query results. Must be globally unique."
  type        = string
}

variable "athena_data_catalog_name" {
  description = "Name of the Athena data source that exposes the federated S3 Tables catalog."
  type        = string
}

variable "athena_reader_principal_arns" {
  description = "IAM principal ARNs (users/roles) that should be granted Lake Formation SELECT/DESCRIBE on the S3 Tables namespace so they can query it from Athena, in addition to the ECS task role."
  type        = list(string)
  default     = []
}

variable "lakeformation_admin_arns" {
  description = "IAM principal ARNs to register as Lake Formation data lake administrators. Should include whoever applies this Terraform config."
  type        = list(string)
  default     = []
}

variable "enable_lakeformation_s3tables_integration" {
  description = "Whether to register the S3 Tables bucket with AWS Glue Data Catalog / Lake Formation so it is queryable from Athena. Requires the applying principal to have Lake Formation admin rights; see DEPLOYMENT.md if this fails."
  type        = bool
  default     = true
}

variable "force_destroy_buckets" {
  description = "Set true in dev to allow `terraform destroy` to delete non-empty S3 buckets. Keep false in prod."
  type        = bool
  default     = false
}

variable "enable_dbt_build_schedule" {
  description = "Whether to create the EventBridge Scheduler schedule that runs the dbt-build ECS task on a recurring basis."
  type        = bool
  default     = true
}

variable "dbt_build_schedule_expression" {
  description = "EventBridge schedule expression for the recurring dbt-build run. Default: daily at 03:00 UTC."
  type        = string
  default     = "cron(0 3 * * ? *)"
}
