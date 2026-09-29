# Batch-renames S3 Tables tables/namespaces, driven by a CSV of rename
# instructions — the core logic behind both the manual CLI below and
# lambda_handler.py, which does the same thing automatically when a CSV
# lands in S3 (see infra/table_admin.tf).
#
# CSV columns (header row required): namespace,name,new_namespace,new_name
#   - namespace, name: current location of the table (required)
#   - new_namespace: leave blank to keep the table in the same namespace
#   - new_name: leave blank to keep the same name (only moving namespace)
#   At least one of new_namespace/new_name must be filled per row, or the
#   row is skipped (nothing to rename) rather than sent to the API.
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
    status: str  # "renamed" | "dry-run" | "skipped" | "failed"
    detail: str = ""


def parse_csv(lines: Iterable[str]) -> list[dict]:
    reader = csv.DictReader(lines)
    required = {"namespace", "name"}
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"CSV is missing required column(s): {sorted(missing)}")
    return list(reader)


def rename_one(client, table_bucket_arn: str, row: dict, dry_run: bool) -> RenameResult:
    namespace = (row.get("namespace") or "").strip()
    name = (row.get("name") or "").strip()
    new_namespace = (row.get("new_namespace") or "").strip()
    new_name = (row.get("new_name") or "").strip()

    if not namespace or not name:
        return RenameResult(namespace, name, new_namespace, new_name, "skipped", "namespace and name are required")
    if not new_namespace and not new_name:
        return RenameResult(
            namespace, name, new_namespace, new_name, "skipped",
            "new_namespace and new_name are both empty — nothing to rename",
        )

    if dry_run:
        return RenameResult(namespace, name, new_namespace, new_name, "dry-run", "would rename, not executed")

    kwargs = {"tableBucketARN": table_bucket_arn, "namespace": namespace, "name": name}
    if new_namespace:
        kwargs["newNamespaceName"] = new_namespace
    if new_name:
        kwargs["newName"] = new_name

    try:
        client.rename_table(**kwargs)
        return RenameResult(namespace, name, new_namespace, new_name, "renamed")
    except botocore.exceptions.ClientError as exc:
        return RenameResult(namespace, name, new_namespace, new_name, "failed", str(exc))


def rename_all(table_bucket_arn: str, rows: list[dict], dry_run: bool = False) -> list[RenameResult]:
    client = boto3.client("s3tables")
    results = []
    for row in rows:
        result = rename_one(client, table_bucket_arn, row, dry_run)
        target = f"{result.new_namespace or result.namespace}.{result.new_name or result.name}"
        detail = f" ({result.detail})" if result.detail else ""
        logger.info("%s.%s -> %s: %s%s", result.namespace, result.name, target, result.status, detail)
        results.append(result)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", required=True, help="Path to the CSV of rename instructions")
    parser.add_argument("--table-bucket-arn", required=True)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate the CSV and log what would happen, without calling RenameTable",
    )
    args = parser.parse_args()

    with open(args.csv, newline="") as f:
        rows = parse_csv(f)

    results = rename_all(args.table_bucket_arn, rows, dry_run=args.dry_run)

    print(json.dumps([dataclasses.asdict(r) for r in results], indent=2))
    if any(r.status == "failed" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
