# Reference ingestion implementation: uploads local CSV file(s) to the bronze
# zone of the landing bucket, partitioned by ingestion date. Replace with the
# production ingestion framework (e.g. one job per source system) as needed.
#
# The task receives AWS permissions from its ECS Task Role; no access keys
# are embedded.
import datetime as dt
import logging
import os
import sys
from pathlib import Path

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest_csv")

SOURCE_NAME = os.getenv("INGEST_SOURCE_NAME", "orders")
INPUT_DIR = Path(os.getenv("INGEST_INPUT_DIR", "ingestion/sample_data"))


def iter_input_files(input_dir: Path):
    if not input_dir.is_dir():
        raise FileNotFoundError(f"INGEST_INPUT_DIR not found: {input_dir}")
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No .csv files found in {input_dir}")
    return files


def upload(s3_client, bucket: str, local_path: Path, source_name: str, run_date: str) -> str:
    # Flat layout (no partition subfolders) so it matches the simple glob in
    # dbt/models/sources.yml (`bronze/<source>/*.csv`).
    key = f"bronze/{source_name}/{run_date}_{local_path.name}"
    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, key)
    s3_client.upload_file(str(local_path), bucket, key)
    return key


def main() -> None:
    bucket = os.getenv("LANDING_BUCKET")
    if not bucket:
        raise SystemExit("LANDING_BUCKET environment variable is required")

    run_date = dt.datetime.utcnow().strftime("%Y-%m-%d")
    files = iter_input_files(INPUT_DIR)

    s3_client = boto3.client("s3")
    uploaded = [upload(s3_client, bucket, f, SOURCE_NAME, run_date) for f in files]

    logger.info("Ingested %d file(s) into s3://%s/bronze/%s/", len(uploaded), bucket, SOURCE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Ingestion failed")
        sys.exit(1)
