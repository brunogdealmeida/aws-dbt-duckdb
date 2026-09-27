# API Gateway (HTTP API, payload format 2.0) Lambda handler for
# `POST /queries` — the entry point of "quack on demand" when hit from AWS
# rather than from local_api.py. Zero third-party dependencies beyond boto3
# (bundled in the Lambda Python runtime), so Terraform zips this file (plus
# common.py) directly with no build step — see infra/query_service.tf.
#
# What it does NOT do, on purpose (see the pending-work note in
# ARCHITECTURE.md's "quack on demand" section): persist anything to
# Postgres. A Lambda running in AWS has no route to the docker-compose
# Postgres on a developer's machine, and no production Postgres is
# provisioned yet. Submission and status only need S3 + ECS, both directly
# reachable from Lambda, so the feature works end-to-end without a database
# — execution *history* and *saved queries* are the two things that do need
# Postgres, and those are local_api.py-only for now.
import json
import logging
import os

import boto3

from common import ecs_run_task_kwargs, new_job_id, query_key, validate_read_only_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("lambda_submit")

s3 = boto3.client("s3")
ecs = boto3.client("ecs")


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "body must be valid JSON"})

    sql = payload.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        return _response(400, {"error": "'sql' is required and must be a non-empty string"})

    try:
        validate_read_only_sql(sql)
    except ValueError as exc:
        return _response(400, {"error": str(exc)})

    job_id = new_job_id()
    bucket = os.environ["LANDING_BUCKET"]
    key = query_key(job_id)
    s3.put_object(Bucket=bucket, Key=key, Body=sql.encode("utf-8"), ContentType="text/plain")

    kwargs = ecs_run_task_kwargs(
        cluster=os.environ["ECS_CLUSTER"],
        task_definition=os.environ["ECS_TASK_DEFINITION_FAMILY"],
        subnets=os.environ["ECS_SUBNETS"].split(","),
        security_groups=os.environ["ECS_SECURITY_GROUPS"].split(","),
        container_name=os.environ.get("ECS_CONTAINER_NAME", "lakehouse"),
        job_id=job_id,
        query_s3_key=key,
        assign_public_ip=os.environ.get("ECS_ASSIGN_PUBLIC_IP", "ENABLED"),
    )
    run_result = ecs.run_task(**kwargs)

    failures = run_result.get("failures")
    if failures:
        logger.error("ecs:RunTask failed for job %s: %s", job_id, failures)
        return _response(502, {"error": "failed to launch query task", "details": failures})

    logger.info("Submitted job %s", job_id)
    return _response(202, {"job_id": job_id, "status": "queued"})
