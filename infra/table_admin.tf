# Batch S3 Tables rename tool: drop a CSV of rename instructions at
# s3://<landing bucket>/table-renames/*.csv and a Lambda renames each row
# via s3tables:RenameTable, writing a per-row results report back to
# table-renames/results/<file>.json. See table_admin/rename_tables.py for
# the CSV format and table_admin/lambda_handler.py for the trigger.
#
# Same S3-event-driven shape as the rest of this stack's "drop a file,
# something reacts" pattern (bronze/ -> dbt sources, queries/ -> query
# runner) rather than yet another HTTP API — there's no result a caller
# needs back synchronously here, so a webhook-shaped trigger has no
# benefit over "upload and check the report a moment later."

data "archive_file" "table_admin_lambda" {
  type       = "zip"
  source_dir = "${path.module}/../table_admin"
  # No leading dot — see the comment on the equivalent line in
  # query_service.tf.
  output_path = "${path.module}/table_admin_lambda.zip"
}

resource "aws_iam_role" "table_admin_lambda" {
  name = "${var.project_name}-${var.environment}-table-admin-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "table_admin_lambda_basic" {
  role       = aws_iam_role.table_admin_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "table_admin_lambda" {
  role = aws_iam_role.table_admin_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadRenameCsvs"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${var.landing_bucket_name}/table-renames/*"]
      },
      {
        Sid      = "WriteRenameReports"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.landing_bucket_name}/table-renames/results/*"]
      },
      {
        Sid    = "RenameTables"
        Effect = "Allow"
        Action = ["s3tables:RenameTable"]
        # s3tables:RenameTable authorizes against the *table* resource
        # (.../bucket/<name>/table/<uuid>), not the table bucket itself —
        # confirmed by hitting AccessDeniedException on this exact action
        # when the Resource here was just the bucket ARN, via the real
        # deployed Lambda triggered by a real S3 upload (not a local test
        # — the table_bucket_arn-only version passed every local/manual
        # test because those ran with the terraform-admin user's broad
        # permissions, not this role's).
        Resource = "${aws_s3tables_table_bucket.lakehouse.arn}/table/*"
      }
    ]
  })
}

resource "aws_lambda_function" "table_admin_rename" {
  function_name = "${var.project_name}-${var.environment}-table-rename"
  role          = aws_iam_role.table_admin_lambda.arn
  handler       = "lambda_handler.handler"
  runtime       = "python3.12"
  timeout       = 60 # a CSV can list many tables; each RenameTable call is a separate API round trip

  filename         = data.archive_file.table_admin_lambda.output_path
  source_code_hash = data.archive_file.table_admin_lambda.output_base64sha256

  environment {
    variables = {
      S3_TABLE_BUCKET_ARN = aws_s3tables_table_bucket.lakehouse.arn
    }
  }
}

resource "aws_lambda_permission" "table_admin_rename_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.table_admin_rename.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.landing.arn
}

resource "aws_s3_bucket_notification" "landing" {
  bucket = aws_s3_bucket.landing.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.table_admin_rename.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "table-renames/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.table_admin_rename_s3_invoke]
}
