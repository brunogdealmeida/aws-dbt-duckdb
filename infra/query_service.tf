# "Quack on demand": an HTTP API (API Gateway + Lambda) that lets any caller
# submit an ad-hoc SELECT and get a result back, executed by the same ECS
# Fargate task/image the dbt pipeline already uses (aws_ecs_task_definition
# .lakehouse, MODE=query — see ingestion/query_runner.py) against the same
# silver/gold Iceberg data. One query = one ephemeral Fargate task, same "no
# idle compute" ethos as the rest of this stack — no warehouse sits around
# waiting for queries.
#
# Deliberately NOT wired to Postgres for metadata/history yet (see
# query_service/lambda_submit.py's module docstring for why) — that part of
# "quack on demand" only exists in the local docker-compose stack
# (query_service/docker-compose.yml) for now. Submission and status work
# fully in this AWS deployment regardless, since both only need S3 + ECS,
# both directly reachable from Lambda.

data "archive_file" "query_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../query_service"
  output_path = "${path.module}/.query_lambda.zip"

  # Local-only pieces of the query_service directory (Postgres-backed API,
  # its Docker image, dev config) — irrelevant to the Lambda handlers and
  # harmless to include, but excluded to keep the zip small and avoid ever
  # accidentally shipping a stray .env.
  excludes = [
    "local_api.py",
    "db.py",
    "schema.sql",
    "Dockerfile",
    "docker-compose.yml",
    "requirements.txt",
    ".env",
    ".env.example",
    "README.md",
    "__pycache__",
  ]
}

resource "aws_iam_role" "query_lambda" {
  name = "${var.project_name}-${var.environment}-query-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "query_lambda_basic" {
  role       = aws_iam_role.query_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "query_lambda" {
  role = aws_iam_role.query_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "QueryArtifactsReadWrite"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject"]
        # Scoped to the queries/ prefix specifically — this role has no
        # access to bronze/ or anything else in the landing bucket.
        Resource = ["arn:aws:s3:::${var.landing_bucket_name}/queries/*"]
      },
      {
        Sid      = "RunQueryTask"
        Effect   = "Allow"
        Action   = ["ecs:RunTask"]
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

locals {
  query_lambda_env = {
    LANDING_BUCKET             = var.landing_bucket_name
    ECS_CLUSTER                = aws_ecs_cluster.lakehouse.name
    ECS_TASK_DEFINITION_FAMILY = aws_ecs_task_definition.lakehouse.family
    ECS_SUBNETS                = join(",", data.aws_subnets.default.ids)
    ECS_SECURITY_GROUPS        = data.aws_security_group.default.id
    ECS_CONTAINER_NAME         = "lakehouse"
    ECS_ASSIGN_PUBLIC_IP       = "ENABLED"
  }
}

resource "aws_lambda_function" "query_submit" {
  function_name = "${var.project_name}-${var.environment}-query-submit"
  role          = aws_iam_role.query_lambda.arn
  handler       = "lambda_submit.handler"
  runtime       = "python3.12"
  timeout       = 10

  filename         = data.archive_file.query_lambda.output_path
  source_code_hash = data.archive_file.query_lambda.output_base64sha256

  environment {
    variables = local.query_lambda_env
  }
}

resource "aws_lambda_function" "query_status" {
  function_name = "${var.project_name}-${var.environment}-query-status"
  role          = aws_iam_role.query_lambda.arn
  handler       = "lambda_status.handler"
  runtime       = "python3.12"
  timeout       = 10

  filename         = data.archive_file.query_lambda.output_path
  source_code_hash = data.archive_file.query_lambda.output_base64sha256

  environment {
    variables = merge(local.query_lambda_env, {
      RESULT_URL_TTL_SECONDS = "3600"
    })
  }
}

resource "aws_apigatewayv2_api" "query" {
  name          = "${var.project_name}-${var.environment}-query"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_stage" "query" {
  api_id      = aws_apigatewayv2_api.query.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_apigatewayv2_integration" "submit" {
  api_id                 = aws_apigatewayv2_api.query.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.query_submit.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "submit" {
  api_id    = aws_apigatewayv2_api.query.id
  route_key = "POST /queries"
  target    = "integrations/${aws_apigatewayv2_integration.submit.id}"
}

resource "aws_apigatewayv2_integration" "status" {
  api_id                 = aws_apigatewayv2_api.query.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.query_status.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "status" {
  api_id    = aws_apigatewayv2_api.query.id
  route_key = "GET /queries/{job_id}"
  target    = "integrations/${aws_apigatewayv2_integration.status.id}"
}

resource "aws_lambda_permission" "submit_invoke" {
  statement_id  = "AllowAPIGatewayInvokeSubmit"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.query_submit.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.query.execution_arn}/*/*/queries"
}

resource "aws_lambda_permission" "status_invoke" {
  statement_id  = "AllowAPIGatewayInvokeStatus"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.query_status.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.query.execution_arn}/*/*/queries/*"
}
