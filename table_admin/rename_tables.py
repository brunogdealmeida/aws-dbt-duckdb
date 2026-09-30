# Batch-renames S3 Tables tables/namespaces, driven by a CSV of rename
# instructions — the core logic behind both the manual CLI below and
# lambda_handler.py, which does the same thing automatically when a CSV
# lands in S3 (see infra/table_admin.tf).
#
# CSV columns (header row required):
#   namespace,name,new_namespace,new_name,table_bucket_arn
#   - namespace, name: current location of the table (required)
#   - new_namespace: leave blank to keep the table in the same namespace
#   - new_name: leave blank to keep the same name (only moving namespace)
#   - table_bucket_arn: which S3 Tables bucket this row targets. Optional
#     per row — falls back to --table-bucket-arn (CLI) /
#     S3_TABLE_BUCKET_ARN (Lambda) when blank or the column is missing
#     entirely, so existing single-bucket CSVs keep working unchanged. A
#     row can override it to target a different table bucket than the
#     default, in the same batch as rows that don't.
#   At least one of new_namespace/new_name must be filled per row, or the
#   row is skipped (nothing to rename) rather than sent to the API.
#
# ** The Lambda's IAM role only has s3tables:RenameTable on the table
#    bucket infra/table_admin.tf deploys (S3_TABLE_BUCKET_ARN) — pointing
#    a row at a *different* table bucket ARN via this column will get
#    AccessDeniedException from AWS unless that IAM policy is widened to
#    cover it too. The CLI has no such restriction: it authorizes as
#    whatever principal is running it. **
#
# Confirmed directly against the real S3 Tables bucket (see ARCHITECTURE.md
# §10): renaming/moving a table this way does not touch its data — same
# underlying Iceberg table, just relocated in the catalog.
#
# ** Renaming a table dbt manages (anything materialized by a model under
#    dbt/models/) moves it out from under the name/schema that model
#    expects — the next `dbt build` won't find it there and will try to
#    recreate it from scratch. Only use this on tables dbt doesn't own, or
#    rename the model at the same time. See ARCHITECTURE.md §10. **
#
# Usage (CLI):
#   python -m table_admin.rename_tables --csv renames.csv --table-bucket-arn <arn>
#   python -m table_admin.rename_tables --csv renames.csv --table-bucket-arn <arn> --dry-run
#   # --table-bucket-arn is only a *default* now — omit it if every row in
#   # the CSV already sets its own table_bucket_arn:
#   python -m table_admin.rename_tables --csv renames.csv
import argparse
import csv
import dataclasses
import json
import logging
import sys
from typing import Iterable

import boto3
import botocore.exceptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rename_tables")


@dataclasses.dataclass
class RenameResult:
    namespace: str
    name: str
    new_namespace: str
    new_name: str
    table_bucket_arn: str
    status: str  # "renamed" | "dry-run" | "skipped" | "failed"
    detail: str = ""


def parse_csv(lines: Iterable[str]) -> list[dict]:
    reader = csv.DictReader(lines)
    required = {"namespace", "name"}
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"CSV is missing required column(s): {sorted(missing)}")
    return list(reader)


def rename_one(client, row: dict, dry_run: bool, default_table_bucket_arn: str = "") -> RenameResult:
    namespace = (row.get("namespace") or "").strip()
    name = (row.get("name") or "").strip()
    new_namespace = (row.get("new_namespace") or "").strip()
    new_name = (row.get("new_name") or "").strip()
    table_bucket_arn = (row.get("table_bucket_arn") or "").strip() or default_table_bucket_arn

    if not namespace or not name:
        return RenameResult(
            namespace, name, new_namespace, new_name, table_bucket_arn,
            "skipped", "namespace and name are required",
        )
    if not table_bucket_arn:
        return RenameResult(
            namespace, name, new_namespace, new_name, table_bucket_arn, "skipped",
            "no table_bucket_arn — set it in this row's column, or pass a default "
            "(--table-bucket-arn for the CLI, S3_TABLE_BUCKET_ARN for the Lambda)",
        )
    if not new_namespace and not new_name:
        return RenameResult(
            namespace, name, new_namespace, new_name, table_bucket_arn, "skipped",
            "new_namespace and new_name are both empty — nothing to rename",
        )

    if dry_run:
        return RenameResult(
            namespace, name, new_namespace, new_name, table_bucket_arn,
            "dry-run", "would rename, not executed",
        )

    kwargs = {"tableBucketARN": table_bucket_arn, "namespace": namespace, "name": name}
    if new_namespace:
        kwargs["newNamespaceName"] = new_namespace
    if new_name:
        kwargs["newName"] = new_name

    try:
        client.rename_table(**kwargs)
        return RenameResult(namespace, name, new_namespace, new_name, table_bucket_arn, "renamed")
    except botocore.exceptions.ClientError as exc:
        return RenameResult(namespace, name, new_namespace, new_name, table_bucket_arn, "failed", str(exc))


def rename_all(rows: list[dict], dry_run: bool = False, default_table_bucket_arn: str = "") -> list[RenameResult]:
    client = boto3.client("s3tables")
    results = []
    for row in rows:
        result = rename_one(client, row, dry_run, default_table_bucket_arn)
        target = f"{result.new_namespace or result.namespace}.{result.new_name or result.name}"
        detail = f" ({result.detail})" if result.detail else ""
        logger.info(
            "[%s] %s.%s -> %s: %s%s",
            result.table_bucket_arn or "<no bucket>", result.namespace, result.name, target, result.status, detail,
        )
        results.append(result)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", required=True, help="Path to the CSV of rename instructions")
    parser.add_argument(
        "--table-bucket-arn",
        default="",
        help="Default table bucket ARN for rows that don't set their own table_bucket_arn column",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate the CSV and log what would happen, without calling RenameTable",
    )
    args = parser.parse_args()

    with open(args.csv, newline="") as f:
        rows = parse_csv(f)

    results = rename_all(rows, dry_run=args.dry_run, default_table_bucket_arn=args.table_bucket_arn)

    print(json.dumps([dataclasses.asdict(r) for r in results], indent=2))
    if any(r.status == "failed" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
