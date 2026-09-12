terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.60"
    }
    awscc = {
      source  = "hashicorp/awscc"
      version = "~> 1.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Used only for awscc_glue_catalog's `federated_catalog` block, which the
# hashicorp/aws provider does not yet expose (see
# https://github.com/hashicorp/terraform-provider-aws/issues/40725).
provider "awscc" {
  region = var.aws_region
}
