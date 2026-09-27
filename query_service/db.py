# Postgres access for local_api.py — plain psycopg2, no ORM (the schema is
# two small tables; SQLAlchemy would be pure overhead here). Not used by the
# Lambda handlers (see the note in lambda_submit.py for why).
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


@contextmanager
def _cursor():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


def insert_execution(job_id: str, sql: str, status: str) -> None:
    with _cursor() as cur:
        cur.execute(
            "INSERT INTO executions (job_id, sql, status) VALUES (%s, %s, %s)",
            (job_id, sql, status),
        )


def upsert_execution_status(job_id: str, status: dict) -> None:
    """Mirrors a status.json payload (from S3, the real source of truth —
    see lambda_status.py) into the local `executions` row, filling in
    whichever fields that status carries."""
    with _cursor() as cur:
        cur.execute(
            """
            UPDATE executions SET
                status = %(status)s,
                started_at = COALESCE(%(started_at)s, started_at),
                finished_at = COALESCE(%(finished_at)s, finished_at),
                row_count = COALESCE(%(row_count)s, row_count),
                result_key = COALESCE(%(result_key)s, result_key),
                error = COALESCE(%(error)s, error)
            WHERE job_id = %(job_id)s
            """,
            {
                "job_id": job_id,
                "status": status.get("status"),
                "started_at": status.get("started_at"),
                "finished_at": status.get("finished_at"),
                "row_count": status.get("row_count"),
                "result_key": status.get("result_key"),
                "error": status.get("error"),
            },
        )


def get_execution(job_id: str) -> dict | None:
    with _cursor() as cur:
        cur.execute("SELECT * FROM executions WHERE job_id = %s", (job_id,))
        return cur.fetchone()


def list_executions(limit: int) -> list:
    with _cursor() as cur:
        cur.execute("SELECT * FROM executions ORDER BY submitted_at DESC LIMIT %s", (limit,))
        return cur.fetchall()


def create_saved_query(name: str, sql: str) -> dict:
    with _cursor() as cur:
        cur.execute(
            "INSERT INTO saved_queries (name, sql) VALUES (%s, %s) RETURNING *",
            (name, sql),
        )
        return cur.fetchone()


def list_saved_queries() -> list:
    with _cursor() as cur:
        cur.execute("SELECT * FROM saved_queries ORDER BY name")
        return cur.fetchall()
