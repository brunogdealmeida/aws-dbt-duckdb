# "Quack on demand", local flavor: an HTTP API you run in docker-compose
# (this container + Postgres — see docker-compose.yml) that submits queries
# to the SAME real ECS Fargate task/S3 Tables data the AWS-deployed API
# (lambda_submit.py / lambda_status.py) does — mirrors the pattern already
# used for local Airflow driving real ECS tasks (see ARCHITECTURE.md).
#
# The one thing this has that the AWS deployment doesn't yet: Postgres.
# GET /queries/{job_id} mirrors the S3 status (the real source of truth —
# see lambda_status.py) into the local `executions` row on every poll,
# which is what makes GET /queries (history) and the saved-queries library
# possible without needing a database reachable from Lambda.
import json
import os

import boto3
import botocore.exceptions
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import db
from common import (
    ecs_run_task_kwargs,
    is_valid_job_id,
    new_job_id,
    query_key,
    result_key,
    status_key,
    validate_read_only_sql,
)

app = FastAPI(title="quack-on-demand (local)")

s3 = boto3.client("s3")
ecs = boto3.client("ecs")


class SubmitQuery(BaseModel):
    sql: str


class SavedQuery(BaseModel):
    name: str
    sql: str


def _bucket() -> str:
    return os.environ["LANDING_BUCKET"]


def _submit(sql: str) -> dict:
    try:
        validate_read_only_sql(sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    job_id = new_job_id()
    key = query_key(job_id)
    s3.put_object(Bucket=_bucket(), Key=key, Body=sql.encode("utf-8"), ContentType="text/plain")

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
    result = ecs.run_task(**kwargs)
    if result.get("failures"):
        raise HTTPException(status_code=502, detail={"error": "failed to launch query task", "details": result["failures"]})

    db.insert_execution(job_id, sql, "queued")
    return {"job_id": job_id, "status": "queued"}


@app.post("/queries", status_code=202)
def submit_query(body: SubmitQuery):
    return _submit(body.sql)


@app.get("/queries/{job_id}")
def get_query(job_id: str):
    if not is_valid_job_id(job_id):
        raise HTTPException(status_code=400, detail="invalid job_id")

    bucket = _bucket()
    try:
        obj = s3.get_object(Bucket=bucket, Key=status_key(job_id))
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
        row = db.get_execution(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown job_id")
        return dict(row)

    status = json.loads(obj["Body"].read())
    db.upsert_execution_status(job_id, status)

    status["job_id"] = job_id
    if status.get("status") == "succeeded":
        status["result_url"] = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": result_key(job_id)},
            ExpiresIn=int(os.environ.get("RESULT_URL_TTL_SECONDS", "3600")),
        )
    return status


@app.get("/queries")
def list_queries(limit: int = 20):
    return [dict(row) for row in db.list_executions(limit)]


@app.post("/saved-queries", status_code=201)
def create_saved_query(body: SavedQuery):
    try:
        validate_read_only_sql(body.sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        return dict(db.create_saved_query(body.name, body.sql))
    except Exception as exc:
        raise HTTPException(status_code=409, detail=f"could not save query {body.name!r}: {exc}")


@app.get("/saved-queries")
def get_saved_queries():
    return [dict(row) for row in db.list_saved_queries()]


@app.post("/saved-queries/{name}/run", status_code=202)
def run_saved_query(name: str):
    matches = [row for row in db.list_saved_queries() if row["name"] == name]
    if not matches:
        raise HTTPException(status_code=404, detail=f"no saved query named {name!r}")
    return _submit(matches[0]["sql"])
