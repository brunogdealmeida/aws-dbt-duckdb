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
