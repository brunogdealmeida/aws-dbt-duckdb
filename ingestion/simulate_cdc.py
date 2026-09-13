# Generates a realistic CDC batch (insert/update/delete rows tagged with a
# `_cdc_op` column) for one or more bronze entities, and uploads it to
# s3://<landing-bucket>/bronze/<entity>/cdc/<timestamp>.csv — read by the
# incremental models in dbt/models/silver/{orders,clients,inventory}.sql via
# the `cdc_merge` custom incremental strategy
# (dbt/macros/incremental_strategy_cdc_merge.sql).
#
# Requires a full-load bronze CSV to already exist locally for each entity
# (ingestion/seed_data/bronze/<entity>/<entity>.csv — see
# generate_seed_data.py) as the pool of valid IDs to mutate/delete, and as
# the basis for realistic new rows. Doesn't touch that file.
#
# Usage:
#   python ingestion/generate_seed_data.py     # once, if not already done
#   python ingestion/simulate_cdc.py --entity orders --entity clients
#   python ingestion/simulate_cdc.py --entity all --update-pct 5 --delete-pct 1 --insert-pct 1
#
# Run it multiple times to simulate several rounds of change — each run is a
# new batch, applied by the next `dbt build` (against `prod`), which reads
# bronze/<entity>/cdc/*.csv as one flat glob. Because of that, this script
# archives previously-uploaded batches to cdc/applied/ before writing a new
# one (see archive_previous_batches), so the glob only ever picks up a single
# batch per dbt build — two batches changing the same key in the same glob
# would give DuckDB's MERGE INTO two source rows for one target row, which it
# resolves by silently picking one rather than erroring (confirmed
# empirically), not necessarily the most recent. Skip with
# --keep-previous-batches only if you want that (usually don't).
import argparse
import datetime
import json
import logging
import os
from pathlib import Path

import boto3
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("simulate_cdc")

SEED_ROOT = Path(__file__).parent / "seed_data" / "bronze"
STATE_FILE = Path(__file__).parent / "seed_data" / ".cdc_state.json"

ENTITIES = ("clients", "inventory", "orders")  # clients/inventory first: orders' inserts reference their ID pools


def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _full_load_path(entity: str) -> Path:
    path = SEED_ROOT / entity / f"{entity}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run `python ingestion/generate_seed_data.py` first "
            f"to create the full-load bronze files this script mutates."
        )
    return path


def simulate_clients(con: duckdb.DuckDBPyConnection, update_pct: float, delete_pct: float, insert_pct: float, next_id: int) -> tuple[Path, int]:
    src = _full_load_path("clients")
    con.execute(f"CREATE OR REPLACE TABLE base AS SELECT * FROM read_csv_auto('{src}')")
    n = con.execute("SELECT count(*) FROM base").fetchone()[0]
    n_insert = max(1, int(n * insert_pct / 100))

    # Single per-row random value shared between the update/delete branches
    # below, so a row can never be tagged both 'U' and 'D' in the same
    # batch — with two independent SAMPLEs, an overlapping row would get
    # deleted and then immediately re-inserted by the merge's "not matched"
    # fallback (since its 'U' copy no longer matches anything after the
    # delete), silently undoing the delete. Confirmed by hitting exactly
    # this against the real S3 Tables tables before adding this fix.
    con.execute("CREATE OR REPLACE TABLE tagged AS SELECT *, random() AS _r FROM base")
    batch_sql = f"""
        SELECT customer_id, name, email, country, signup_date, segment, 'D' AS _cdc_op
        FROM tagged WHERE _r < {delete_pct / 100}
        UNION ALL
        SELECT customer_id, name, email, country, signup_date, segment, 'U' AS _cdc_op
        FROM tagged WHERE _r >= {delete_pct / 100} AND _r < {(delete_pct + update_pct) / 100}
        UNION ALL
        SELECT
            {next_id} + i AS customer_id,
            'Client ' || ({next_id} + i) AS name,
            'client' || ({next_id} + i) || '@example.com' AS email,
            (['BR', 'US', 'PT', 'AR', 'MX'])[1 + (random() * 4)::INT] AS country,
            current_date AS signup_date,
            (['retail', 'wholesale', 'vip'])[1 + (random() * 2)::INT] AS segment,
            'I' AS _cdc_op
        FROM range(0, {n_insert}) t(i)
    """
    out = SEED_ROOT / "clients" / "cdc" / f"{_timestamp()}.csv"
    _write(con, batch_sql, out)
    return out, next_id + n_insert


def simulate_inventory(con: duckdb.DuckDBPyConnection, update_pct: float, delete_pct: float, insert_pct: float, next_id: int) -> tuple[Path, int]:
    src = _full_load_path("inventory")
    con.execute(f"CREATE OR REPLACE TABLE base AS SELECT * FROM read_csv_auto('{src}')")
    n = con.execute("SELECT count(*) FROM base").fetchone()[0]
    n_insert = max(1, int(n * insert_pct / 100))

    # See the comment in simulate_clients() — single per-row random value so
    # update/delete are mutually exclusive.
    con.execute("CREATE OR REPLACE TABLE tagged AS SELECT *, random() AS _r FROM base")
    batch_sql = f"""
        SELECT product_id, warehouse_id, category, quantity_on_hand, unit_cost, last_restock_date, status, 'D' AS _cdc_op
        FROM tagged WHERE _r < {delete_pct / 100}
        UNION ALL
        SELECT product_id, warehouse_id, category,
               (random() * 500)::INT AS quantity_on_hand,
               round(unit_cost * (0.9 + random() * 0.2), 2) AS unit_cost,
               current_date AS last_restock_date,
               status, 'U' AS _cdc_op
        FROM tagged WHERE _r >= {delete_pct / 100} AND _r < {(delete_pct + update_pct) / 100}
        UNION ALL
        SELECT
            {next_id} + i AS product_id,
            (1 + (random() * 9)::INT) AS warehouse_id,
            (['Electronics', 'Grocery', 'Apparel', 'Toys', 'Home'])[1 + (random() * 4)::INT] AS category,
            (random() * 500)::INT AS quantity_on_hand,
            round(1 + random() * 499, 2) AS unit_cost,
            current_date AS last_restock_date,
            'active' AS status,
            'I' AS _cdc_op
        FROM range(0, {n_insert}) t(i)
    """
    out = SEED_ROOT / "inventory" / "cdc" / f"{_timestamp()}.csv"
    _write(con, batch_sql, out)
    return out, next_id + n_insert


def simulate_orders(
    con: duckdb.DuckDBPyConnection,
    update_pct: float,
    delete_pct: float,
    insert_pct: float,
    next_id: int,
    max_customer_id: int,
    max_product_id: int,
) -> tuple[Path, int]:
    src = _full_load_path("orders")
    con.execute(f"CREATE OR REPLACE TABLE base AS SELECT * FROM read_csv_auto('{src}')")
    con.execute(f"CREATE OR REPLACE TABLE inv AS SELECT * FROM read_csv_auto('{_full_load_path('inventory')}')")
    n = con.execute("SELECT count(*) FROM base").fetchone()[0]
    n_insert = max(1, int(n * insert_pct / 100))

    # See the comment in simulate_clients() — single per-row random value so
    # update/delete are mutually exclusive.
    con.execute("CREATE OR REPLACE TABLE tagged AS SELECT *, random() AS _r FROM base")
    batch_sql = f"""
        SELECT order_id, customer_id, product_id, quantity, order_date, amount, status, 'D' AS _cdc_op
        FROM tagged WHERE _r < {delete_pct / 100}
        UNION ALL
        -- status changes and quantity edits on existing orders
        SELECT order_id, customer_id, product_id,
               quantity,
               current_date AS order_date,
               amount,
               (['Paid', 'Cancelled', 'Refunded'])[1 + (random() * 2)::INT] AS status,
               'U' AS _cdc_op
        FROM tagged WHERE _r >= {delete_pct / 100} AND _r < {(delete_pct + update_pct) / 100}
        UNION ALL
        -- brand new orders, referencing real (existing or already-inserted) customer/product IDs
        SELECT
            {next_id} + o.i AS order_id,
            (1 + (random() * {max_customer_id - 1})::BIGINT) AS customer_id,
            o.product_id,
            (1 + (random() * 9)::INT) AS quantity,
            current_date AS order_date,
            round((1 + (random() * 9)::INT) * inv.unit_cost * (0.8 + random() * 0.5), 2) AS amount,
            (['Paid', 'Pending'])[1 + (random() * 1)::INT] AS status,
            'I' AS _cdc_op
        FROM (
            SELECT i, (1 + (random() * {max_product_id - 1})::BIGINT) AS product_id
            FROM range(0, {n_insert}) t(i)
        ) o
        JOIN inv ON inv.product_id = o.product_id
    """
    out = SEED_ROOT / "orders" / "cdc" / f"{_timestamp()}.csv"
    _write(con, batch_sql, out)
    return out, next_id + n_insert


def _timestamp() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write(con: duckdb.DuckDBPyConnection, select_sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({select_sql}) TO '{out_path}' (HEADER, DELIMITER ',')")
    counts = con.execute(f"SELECT _cdc_op, count(*) FROM read_csv_auto('{out_path}') GROUP BY 1 ORDER BY 1").fetchall()
    logger.info("Wrote %s — %s", out_path, dict(counts))


def upload(path: Path, bucket: str, entity: str) -> None:
    key = f"bronze/{entity}/cdc/{path.name}"
    logger.info("Uploading %s to s3://%s/%s", path, bucket, key)
    boto3.client("s3").upload_file(str(path), bucket, key)


def archive_previous_batches(bucket: str, entity: str) -> None:
    # The dbt source reads bronze/<entity>/cdc/*.csv as one flat glob — every
    # run reprocesses every batch ever uploaded, not just new ones. If the
    # same key was changed in two different historical batches, the merge
    # source ends up with two rows for that key and DuckDB's MERGE INTO
    # silently picks one (empirically: the first, not necessarily the most
    # recent) rather than erroring — confirmed with a direct MERGE INTO test.
    # Moving each batch out to cdc/applied/ (one level deeper, so it no
    # longer matches the flat cdc/*.csv glob) right before writing the next
    # one keeps exactly one batch in the glob's path per dbt build, avoiding
    # the ambiguity. Assumes the normal workflow: generate -> dbt build ->
    # generate -> dbt build (skip with --keep-previous-batches otherwise).
    prefix = f"bronze/{entity}/cdc/"
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".csv"):
            continue
        dest_key = f"{prefix}applied/{key[len(prefix):]}"
        logger.info("Archiving previous batch s3://%s/%s -> %s", bucket, key, dest_key)
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=dest_key)
        s3.delete_object(Bucket=bucket, Key=key)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entity", action="append", choices=[*ENTITIES, "all"], default=[], help="Repeatable; default: all")
    parser.add_argument("--update-pct", type=float, default=2.0, help="%% of existing rows to UPDATE")
    parser.add_argument("--delete-pct", type=float, default=0.5, help="%% of existing rows to DELETE")
    parser.add_argument("--insert-pct", type=float, default=0.5, help="New rows as %% of current row count")
    parser.add_argument("--no-upload", action="store_true", help="Only write locally, skip the S3 upload")
    parser.add_argument(
        "--keep-previous-batches",
        action="store_true",
        help="Don't archive previously-uploaded CDC batches to cdc/applied/ before uploading the new one "
        "(see archive_previous_batches) — only useful if you intentionally want several batches to "
        "accumulate before the next dbt build, which risks ambiguous merges for keys changed twice",
    )
    args = parser.parse_args()

    entities = set(ENTITIES) if (not args.entity or "all" in args.entity) else set(args.entity)
    bucket = os.getenv("LANDING_BUCKET")

    con = duckdb.connect()
    state = _load_state()
    outputs: list[tuple[str, Path]] = []

    if "clients" in entities:
        next_id = state.get("clients_next_id", 100_001)
        path, next_id = simulate_clients(con, args.update_pct, args.delete_pct, args.insert_pct, next_id)
        state["clients_next_id"] = next_id
        outputs.append(("clients", path))

    if "inventory" in entities:
        next_id = state.get("inventory_next_id", 1_000_001)
        path, next_id = simulate_inventory(con, args.update_pct, args.delete_pct, args.insert_pct, next_id)
        state["inventory_next_id"] = next_id
        outputs.append(("inventory", path))

    if "orders" in entities:
        next_id = state.get("orders_next_id", 5_000_001)
        max_customer_id = state.get("clients_next_id", 100_001) - 1
        max_product_id = state.get("inventory_next_id", 1_000_001) - 1
        path, next_id = simulate_orders(con, args.update_pct, args.delete_pct, args.insert_pct, next_id, max_customer_id, max_product_id)
        state["orders_next_id"] = next_id
        outputs.append(("orders", path))

    _save_state(state)

    if bucket and not args.no_upload:
        for entity, path in outputs:
            if not args.keep_previous_batches:
                archive_previous_batches(bucket, entity)
            upload(path, bucket, entity)
    elif not args.no_upload:
        logger.info("LANDING_BUCKET not set — skipping upload. Files are local; sync manually when ready:")
        logger.info("  aws s3 sync %s s3://<landing-bucket>/bronze/", SEED_ROOT)


if __name__ == "__main__":
    main()
