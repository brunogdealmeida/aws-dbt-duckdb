import datetime
import glob
import json
import logging
import os
import subprocess
import sys
import urllib.request
import uuid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("entrypoint")

mode = os.getenv("MODE", "dbt")

if mode == "dbt":
    cmd = ["dbt", "run"]
elif mode == "dbt-test":
    cmd = ["dbt", "test"]
elif mode == "dbt-build":
    cmd = ["dbt", "build"]
elif mode == "ingest":
    cmd = [sys.executable, "-m", "ingestion.ingest_csv"]
else:
    raise SystemExit(f"Unsupported MODE={mode}")


def _run_id() -> str:
    # Prefer the ECS task ID (matches the CloudWatch Logs stream name), so
    # logs in S3 and in CloudWatch can be correlated by the same identifier.
    metadata_uri = os.getenv("ECS_CONTAINER_METADATA_URI_V4")
    if metadata_uri:
        try:
            with urllib.request.urlopen(f"{metadata_uri}/task", timeout=2) as resp:
                task_arn = json.load(resp).get("TaskARN", "")
            if task_arn:
                return task_arn.rsplit("/", 1)[-1]
        except Exception:
            logger.warning("Could not read ECS task metadata; falling back to a random run id", exc_info=True)
    return uuid.uuid4().hex


def upload_dbt_logs() -> None:
    """Uploads dbt's log files (dbt/logs/*) to S3 — best-effort, never raises.

    The ECS task filesystem is ephemeral, so without this, `dbt/logs/dbt.log`
    (dbt's own verbose log, distinct from the stdout/stderr CloudWatch
    already captures) is lost the moment the task stops — including on
    failed runs, which is usually exactly when you need it.
    """
    bucket = os.getenv("DBT_LOG_BUCKET")
    if not bucket:
        return

    # dbt_project.yml sits at /app (WORKDIR) with no `log-path` override, so
    # dbt's default `logs/` resolves relative to /app — not `dbt/logs/`.
    log_files = [p for p in glob.glob("logs/*") if os.path.isfile(p)]
    if not log_files:
        logger.info("No dbt log files found to upload (logs/ empty or missing)")
        return

    import boto3

    run_id = _run_id()
    date_prefix = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    s3_client = boto3.client("s3")

    for path in log_files:
        key = f"dbt/{date_prefix}/{mode}/{run_id}/{os.path.basename(path)}"
        try:
            s3_client.upload_file(path, bucket, key)
            logger.info("Uploaded %s to s3://%s/%s", path, bucket, key)
        except Exception:
            logger.exception("Failed to upload %s to s3://%s/%s", path, bucket, key)


if __name__ == "__main__":
    try:
        result = subprocess.run(cmd)
    finally:
        upload_dbt_logs()
    sys.exit(result.returncode)
