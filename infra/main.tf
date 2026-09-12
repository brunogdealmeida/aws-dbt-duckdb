data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  github_owner     = split("/", var.github_repository)[0]
  github_repo_name = split("/", var.github_repository)[1]
}

resource "aws_ecr_repository" "lakehouse" {
  name                 = "${var.project_name}-${var.environment}"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecs_cluster" "lakehouse" {
  name = "${var.project_name}-${var.environment}"
}

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/${var.project_name}/${var.environment}"
  retention_in_days = 30
}

resource "aws_iam_role" "ecs_execution" {
  name = "${var.project_name}-${var.environment}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name = "${var.project_name}-${var.environment}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task" {
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.landing_bucket_name}",
          "arn:aws:s3:::${var.landing_bucket_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.dbt_logs_bucket_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3tables:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "lakeformation:GetDataAccess"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_ecs_task_definition" "lakehouse" {
  family                   = "${var.project_name}-${var.environment}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.container_cpu
  memory                   = var.container_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "lakehouse"
    image     = "${aws_ecr_repository.lakehouse.repository_url}:bootstrap"
    essential = true

    environment = [
      { name = "AWS_REGION", value = var.aws_region },
      { name = "MODE", value = "dbt" },
      { name = "DBT_TARGET", value = "prod" },
      { name = "LANDING_BUCKET", value = var.landing_bucket_name },
      { name = "S3_TABLE_BUCKET", value = var.s3_tables_bucket_name },
      { name = "S3_TABLES_NAMESPACE", value = var.s3_tables_namespace },
      { name = "AWS_ACCOUNT_ID", value = data.aws_caller_identity.current.account_id },
      { name = "DBT_LOG_BUCKET", value = var.dbt_logs_bucket_name }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ecs.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "lakehouse"
      }
    }
  }])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}

# GitHub OIDC provider
data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.github.certificates[0].sha1_fingerprint]
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    effect = "Allow"

    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Every job that assumes this role specifies `environment: <env>` (see
    # deploy.yml / terraform.yml), which makes GitHub mint the OIDC token
    # with an environment-scoped `sub` claim instead of the more commonly
    # documented ref-scoped one. Allow both so this doesn't break if a
    # future workflow assumes the role without an `environment:` key.
    #
    # GitHub also appends an immutable numeric ID to the owner and/or repo
    # name (e.g. "octo-org@123/octo-repo@456" instead of "octo-org/octo-repo")
    # to make the subject resistant to rename-based hijacking — confirmed
    # empirically against a live token, since this isn't in GitHub's own
    # trust-policy examples. The trailing `*` on each segment matches with
    # or without that suffix.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${local.github_owner}*/${local.github_repo_name}*:environment:${var.environment}",
        "repo:${local.github_owner}*/${local.github_repo_name}*:ref:refs/heads/${var.github_branch}"
      ]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name               = var.deploy_role_name
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

# terraform.yml's apply job uses this same role to run `terraform apply`,
# which needs to manage every service this config touches (S3, S3 Tables,
# Glue, Lake Formation, Athena, ECS, ECR, IAM, CloudWatch...) plus the
# Terraform state bucket/lock table — not just the narrower ECR/ECS actions
# below. AdministratorAccess is the pragmatic choice for dev (least fragile
# against new resource types); for prod, replace this with a scoped policy
# and require GitHub Environment reviewers before terraform.yml's apply job
# can run.
resource "aws_iam_role_policy_attachment" "github_deploy_admin" {
  role       = aws_iam_role.github_deploy.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid = "ECRPush"

    actions = [
      "ecr:GetAuthorizationToken"
    ]

    resources = ["*"]
  }

  statement {
    sid = "ECRRepository"

    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecr:BatchGetImage"
    ]

    resources = [aws_ecr_repository.lakehouse.arn]
  }

  statement {
    sid = "ECSDeploy"

    actions = [
      "ecs:DescribeTaskDefinition",
      "ecs:RegisterTaskDefinition",
      "ecs:DeregisterTaskDefinition",
      "ecs:RunTask",
      "ecs:DescribeTasks",
      "ecs:StopTask"
    ]

    resources = ["*"]
  }

  statement {
    sid = "PassECSTaskRoles"

    actions = ["iam:PassRole"]

    resources = [
      aws_iam_role.ecs_execution.arn,
      aws_iam_role.ecs_task.arn
    ]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy.json
}
