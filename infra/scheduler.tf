# Runs `dbt build` on a schedule via EventBridge Scheduler -> ECS RunTask,
# reusing the exact same task definition/network setup the deploy workflow's
# manual `workflow_dispatch` uses. Cheaper and simpler than standing up an
# orchestrator for a single recurring batch job — see DEPLOYMENT.md for the
# cost comparison against MWAA / self-hosted Airflow.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_security_group" "default" {
  vpc_id = data.aws_vpc.default.id
  name   = "default"
}

resource "aws_iam_role" "scheduler" {
  count = var.enable_dbt_build_schedule ? 1 : 0

  name = "${var.project_name}-${var.environment}-scheduler"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  count = var.enable_dbt_build_schedule ? 1 : 0

  role = aws_iam_role.scheduler[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunTask"
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        # Family-scoped wildcard, not a specific revision: the task
        # definition ARN passed below omits the revision number so ECS
        # always resolves the latest ACTIVE one (deploy.yml registers new
        # revisions outside Terraform — see main.tf's ignore_changes on
        # container_definitions).
        Resource = "arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.lakehouse.family}:*"
      },
      {
        Sid      = "PassEcsRoles"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.ecs_execution.arn, aws_iam_role.ecs_task.arn]
      }
    ]
  })
}

resource "aws_scheduler_schedule" "dbt_build" {
  count = var.enable_dbt_build_schedule ? 1 : 0

  name       = "${var.project_name}-${var.environment}-dbt-build"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.dbt_build_schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_ecs_cluster.lakehouse.arn
    role_arn = aws_iam_role.scheduler[0].arn

    ecs_parameters {
      task_definition_arn = "arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.lakehouse.family}"
      launch_type          = "FARGATE"
      task_count           = 1

      network_configuration {
        subnets          = data.aws_subnets.default.ids
        security_groups  = [data.aws_security_group.default.id]
        assign_public_ip = true
      }
    }

    input = jsonencode({
      containerOverrides = [{
        name        = "lakehouse"
        environment = [{ name = "MODE", value = "dbt-build" }]
      }]
    })
  }
}
