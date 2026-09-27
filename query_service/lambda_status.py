# API Gateway (HTTP API, payload format 2.0) Lambda handler for
# `GET /queries/{job_id}` — polls the status query_runner.py wrote to S3.
# Status.json IS the source of truth (not Postgres, not the ECS task's own
# state) precisely so this endpoint needs nothing but S3 read access — see
# the note in lambda_submit.py about why Postgres isn't in this path yet.
import json
import os

import boto3
import botocore.exceptions

from common import is_valid_job_id, query_key, result_key, status_key

s3 = boto3.client("s3")


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def handler(event, context):
    job_id = (event.get("pathParameters") or {}).get("job_id", "")
    if not is_valid_job_id(job_id):
        return _response(400, {"error": "invalid job_id"})

    bucket = os.environ["LANDING_BUCKET"]

    try:
        obj = s3.get_object(Bucket=bucket, Key=status_key(job_id))
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
        # No status.json yet: the ECS task may still be starting up (cold
        # start is typically 30-60s on Fargate — see ARCHITECTURE.md), or
        # job_id was never submitted at all. Only query.sql existing
        # distinguishes the two without needing a database.
        if _object_exists(bucket, query_key(job_id)):
            return _response(200, {"job_id": job_id, "status": "queued"})
        return _response(404, {"error": "unknown job_id"})

    status = json.loads(obj["Body"].read())
    status["job_id"] = job_id

    if status.get("status") == "succeeded":
        status["result_url"] = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": result_key(job_id)},
            ExpiresIn=int(os.environ.get("RESULT_URL_TTL_SECONDS", "3600")),
        )

    return _response(200, status)
