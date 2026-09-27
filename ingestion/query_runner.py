# "Quack on demand": runs one ad-hoc SQL query against the S3 Tables
# lakehouse (same silver/gold Iceberg data dbt writes) and uploads the
# result — invoked as MODE=query inside the same ECS task/image the dbt
# pipeline already uses (see entrypoint.py), triggered per-query via
# `ecs:RunTask` container overrides by query_service/lambda_submit.py (in
# AWS) or query_service/local_api.py (running locally against real AWS).
#
# Contract with the caller (see query_service/common.py, the single source
# of truth for this layout):
#   in:  s3://<LANDING_BUCKET>/queries/<job_id>/query.sql   (the SQL text)
#   out: s3://<LANDING_BUCKET>/queries/<job_id>/status.json (written at
#        start AND at the end — polling status.json is how a caller that
#        isn't watching the ECS task directly knows the query is done)
#   out: s3://<LANDING_BUCKET>/queries/<job_id>/result.parquet (only on
#        success)
#
# Same DuckDB<->S3 Tables ATTACH this repo already uses in
# dbt/profiles.yml's `prod` target (credential_chain off the ECS task
# role — no keys anywhere) — replicated here via the Python API instead of
# dbt's `attach` profile config, since this isn't a dbt run. After ATTACH,
# `SET search_path` so a submitted query can write bare `silver.orders` /
# `fct_portfolio_revenue`, matching what the Athena docs in
# ARCHITECTURE.md already tell people to type — `USE s3_tables` alone does
# NOT work here (confirmed directly: DuckDB's USE sets a schema, and a bare
# catalog with no default schema has none to set).
#
# query_service/common.py is copied into this image too (see dbt/Dockerfile)
# purely for validate_read_only_sql() and the S3 key-layout helpers, so the
# safety guard and path layout can't drift between this runner and the
# Lambda handlers that submit to it.
import datetime
import json
import logging
import os
import sys

import boto3
import duckdb

from query_service.common import result_key as build_result_key
from query_service.common import status_key as build_status_key
from query_service.common import validate_read_only_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("query_runner")


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _write_status(s3_client, bucket: str, job_id: str, status: dict) -> None:
    key = build_status_key(job_id)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(status).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote s3://%s/%s (status=%s)", bucket, key, status.get("status"))


def attach_lakehouse(con: duckdb.DuckDBPyConnection) -> None:
    region = os.environ["AWS_REGION"]
    account_id = os.environ["AWS_ACCOUNT_ID"]
    table_bucket = os.environ["S3_TABLE_BUCKET"]
    silver_ns = os.environ.get("S3_TABLES_NAMESPACE", "silver")
    gold_ns = os.environ.get("S3_TABLES_GOLD_NAMESPACE", "gold")

    con.execute("INSTALL httpfs; INSTALL aws; INSTALL iceberg; LOAD httpfs; LOAD aws; LOAD iceberg;")
    con.execute("CREATE SECRET s3_tables_secret (TYPE s3, PROVIDER credential_chain, REGION ?);", [region])
    con.execute(
        f"ATTACH IF NOT EXISTS 'arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}' "
        f"AS s3_tables (TYPE iceberg, ENDPOINT_TYPE s3_tables, SECRET s3_tables_secret);"
    )
    # `USE s3_tables` alone doesn't work — DuckDB's USE sets a *schema*, and
    # a catalog with no default schema has none to set (confirmed directly:
    # "Catalog Error: SET schema: No catalog + schema named 's3_tables'
    # found"). search_path is what actually makes bare `silver.orders` /
    # `fct_portfolio_revenue` resolve without the `s3_tables.` prefix,
    # matching the unprefixed names ARCHITECTURE.md's Athena examples use.
    con.execute(f"SET search_path = 's3_tables.{silver_ns},s3_tables.{gold_ns}';")


def run(job_id: str, query_s3_key: str) -> None:
    bucket = os.environ["LANDING_BUCKET"]
    s3_client = boto3.client("s3")

    started_at = _now()
    _write_status(s3_client, bucket, job_id, {"status": "running", "started_at": started_at})

    sql = s3_client.get_object(Bucket=bucket, Key=query_s3_key)["Body"].read().decode("utf-8")

    try:
        validate_read_only_sql(sql)

        con = duckdb.connect()
        attach_lakehouse(con)

        result_path = "/tmp/result.parquet"
        # COPY (query) TO ... runs the submitted SQL as a subquery, so it
        # only ever executes as a read — even if validate_read_only() had a
        # gap, COPY's own grammar can't express a DDL/DML statement here.
        con.execute(f"COPY ({sql}) TO '{result_path}' (FORMAT PARQUET)")
        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{result_path}')").fetchone()[0]

        r_key = build_result_key(job_id)
        s3_client.upload_file(result_path, bucket, r_key)

        finished_at = _now()
        _write_status(s3_client, bucket, job_id, {
            "status": "succeeded",
            "started_at": started_at,
            "finished_at": finished_at,
            "row_count": row_count,
            "result_key": r_key,
        })
        logger.info("Query %s succeeded: %d rows", job_id, row_count)

    except Exception as exc:
        logger.exception("Query %s failed", job_id)
        _write_status(s3_client, bucket, job_id, {
            "status": "failed",
            "started_at": started_at,
            "finished_at": _now(),
            "error": str(exc),
        })


if __name__ == "__main__":
    job_id_env = os.environ.get("JOB_ID")
    query_key_env = os.environ.get("QUERY_S3_KEY")
    if not job_id_env or not query_key_env:
        raise SystemExit("MODE=query requires JOB_ID and QUERY_S3_KEY")
    # A failed *query* (bad SQL, missing table, a rejected non-SELECT
    # statement) is an expected outcome recorded in status.json, not an
    # infra failure — always exit 0 so the ECS task itself shows SUCCESS
    # regardless. status.json, not the task's exit code, is the source of
    # truth callers poll (see query_service/lambda_status.py).
    run(job_id_env, query_key_env)
    sys.exit(0)
