# S3-triggered Lambda: fires when a CSV lands in
# s3://<LANDING_BUCKET>/table-renames/*.csv (see infra/table_admin.tf), and
# batch-renames S3 Tables tables/namespaces per row — see rename_tables.py
# for the CSV format and the dbt-ownership warning.
#
# S3_TABLE_BUCKET_ARN (set in infra/table_admin.tf) is only the *default*
# table bucket, used for any row that doesn't set its own table_bucket_arn
# column — it's optional here specifically so a CSV can target a different
# table bucket per row instead of always the one Terraform deploys. Doing
# that still needs this Lambda's IAM role to actually have
# s3tables:RenameTable on that other bucket, or AWS rejects it with
# AccessDeniedException regardless of what the CSV says (see
# rename_tables.py's module docstring).
#
# Writes a results report next to the trigger, at
# table-renames/results/<input-csv-name>.json, so outcomes are reviewable
# without digging through CloudWatch — same "write the outcome to S3, not
# just logs" pattern as ingestion/query_runner.py's status.json.
import dataclasses
import json
import logging
import os
import posixpath
import urllib.parse

import boto3

from rename_tables import parse_csv, rename_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("table_admin.lambda_handler")

s3 = boto3.client("s3")


def handler(event, context):
    default_table_bucket_arn = os.environ.get("S3_TABLE_BUCKET_ARN", "")

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        # S3 event keys are URL-encoded (e.g. spaces as '+') — unquoting is
        # required for any key that isn't already URL-safe.
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        logger.info("Processing s3://%s/%s", bucket, key)

        obj = s3.get_object(Bucket=bucket, Key=key)
        rows = parse_csv(obj["Body"].read().decode("utf-8").splitlines())

        results = rename_all(rows, default_table_bucket_arn=default_table_bucket_arn)

        report_key = posixpath.join("table-renames", "results", posixpath.basename(key) + ".json")
        s3.put_object(
            Bucket=bucket,
            Key=report_key,
            Body=json.dumps([dataclasses.asdict(r) for r in results], indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Wrote report to s3://%s/%s", bucket, report_key)
