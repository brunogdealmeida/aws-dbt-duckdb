# Partial backend configuration: bucket/key/region/dynamodb_table are supplied
# at `terraform init` time via -backend-config (see backend.hcl.example), so the
# same code can be initialized against different state stores per environment
# without editing this file.
terraform {
  backend "s3" {
    encrypt = true
  }
}
