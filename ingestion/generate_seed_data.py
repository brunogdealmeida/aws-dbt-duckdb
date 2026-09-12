# Generates large, intentionally messy CSV fixtures for the bronze layer
# (orders, clients, inventory) using DuckDB itself — fast enough to produce
# millions of rows in seconds, and keeps the tool stack consistent with the
# rest of the project.
#
# The three entities are relationally consistent by construction, so the
# silver models can be joined into a gold layer later:
#   orders.customer_id -> clients.customer_id
#   orders.product_id  -> inventory.product_id
# orders.amount is derived from inventory.unit_cost * quantity (with noise),
# so revenue/margin roll-ups in gold will actually reconcile against
# inventory costs instead of being random noise.
#
# Output layout mirrors the S3 landing bucket so it can be pushed as-is:
#   ingestion/seed_data/bronze/<source>/<source>.csv
#
# Usage:
#   python ingestion/generate_seed_data.py
#   aws s3 sync ingestion/seed_data/bronze s3://<landing-bucket>/bronze/
import logging
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("generate_seed_data")

OUTPUT_ROOT = Path(__file__).parent / "seed_data" / "bronze"

ORDERS_ROWS = 5_000_000
CLIENTS_ROWS = 100_000
INVENTORY_ROWS = 1_000_000


def write_table_to_csv(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY {table} TO '{path}' (HEADER, DELIMITER ',')")
    rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    logger.info("Wrote %s (%d rows, %.1f MB)", path, rows, path.stat().st_size / 1_048_576)


def main() -> None:
    con = duckdb.connect()
    con.execute("SELECT setseed(0.42)")  # reproducible runs

    logger.info("Generating clients (%d rows)...", CLIENTS_ROWS)
    con.execute(f"""
        CREATE OR REPLACE TABLE clients_seed AS
        SELECT
            i AS customer_id,
            'Client ' || i AS name,
            CASE WHEN random() < 0.02 THEN NULL
                 ELSE 'client' || i || '@example.com' END AS email,
            (['BR', 'US', 'PT', 'AR', 'MX', 'CL', 'CO', 'ES']
             )[1 + (random() * 7)::INT] AS country,
            (DATE '2020-01-01' + (random() * 2450)::INT) AS signup_date,
            CASE WHEN random() < 0.03 THEN NULL
                 ELSE (['retail', 'Retail', 'wholesale', 'VIP', 'vip']
                       )[1 + (random() * 4)::INT] END AS segment
        FROM range(1, {CLIENTS_ROWS + 1}) t(i)
    """)
    write_table_to_csv(con, "clients_seed", OUTPUT_ROOT / "clients" / "clients.csv")

    logger.info("Generating inventory (%d rows)...", INVENTORY_ROWS)
    con.execute(f"""
        CREATE OR REPLACE TABLE inventory_seed AS
        SELECT
            i AS product_id,
            (1 + (random() * 9)::INT) AS warehouse_id,
            (['Electronics', 'Grocery', 'Apparel', 'Toys', 'Home']
             )[1 + (random() * 4)::INT] AS category,
            (random() * 500)::INT AS quantity_on_hand,
            round(1 + random() * 499, 2) AS unit_cost,
            (DATE '2025-01-01' + (random() * 620)::INT) AS last_restock_date,
            CASE WHEN random() < 0.02 THEN NULL
                 ELSE (['active', 'ACTIVE', 'Active', 'discontinued', 'Discontinued']
                       )[1 + (random() * 4)::INT] END AS status
        FROM range(1, {INVENTORY_ROWS + 1}) t(i)
    """)
    write_table_to_csv(con, "inventory_seed", OUTPUT_ROOT / "inventory" / "inventory.csv")

    logger.info("Generating orders (%d rows, joined to clients/inventory)...", ORDERS_ROWS)
    con.execute(f"""
        CREATE OR REPLACE TABLE orders_seed AS
        SELECT
            o.order_id,
            CASE WHEN random() < 0.005 THEN NULL ELSE o.customer_id END AS customer_id,
            CASE WHEN random() < 0.01 THEN NULL ELSE o.product_id END AS product_id,
            o.quantity,
            o.order_date,
            -- derived from the product's real unit_cost so gold-layer
            -- revenue/margin calculations reconcile against inventory
            round(o.quantity * inv.unit_cost * (0.8 + random() * 0.5), 2) AS amount,
            CASE WHEN random() < 0.01 THEN NULL
                 ELSE (['Paid', 'paid', 'PAID', 'Pending', 'pending',
                        'Cancelled', 'cancelled', 'Refunded', 'refunded']
                       )[1 + (random() * 8)::INT] END AS status
        FROM (
            SELECT
                i AS order_id,
                (1 + (random() * {CLIENTS_ROWS - 1})::BIGINT) AS customer_id,
                (1 + (random() * {INVENTORY_ROWS - 1})::BIGINT) AS product_id,
                (1 + (random() * 9)::INT) AS quantity,
                (DATE '2024-01-01' + (random() * 985)::INT) AS order_date
            FROM range(1, {ORDERS_ROWS + 1}) t(i)
        ) o
        JOIN inventory_seed inv ON inv.product_id = o.product_id
    """)
    write_table_to_csv(con, "orders_seed", OUTPUT_ROOT / "orders" / "orders.csv")


if __name__ == "__main__":
    main()
