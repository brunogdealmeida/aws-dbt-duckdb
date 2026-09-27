# Pure-stdlib helpers shared between the AWS Lambda handlers
# (lambda_submit.py / lambda_status.py, deployed with zero extra
# dependencies — only what the Lambda Python runtime ships, boto3 included)
# and local_api.py (the local docker-compose API). Keeping this stdlib-only
# is what lets Terraform zip the Lambda source directly with no build/pip
# step (see infra/query_service.tf's `archive_file`).
#
# This is the single source of truth for the S3 key layout
# (ingestion/query_runner.py, the piece that actually runs inside ECS,
# mirrors these same paths — kept in sync by hand since it can't import
# this module, running inside a separate Docker image).
import re
import uuid


def new_job_id() -> str:
    return uuid.uuid4().hex


# Read-only tool: a query execution API that could run arbitrary DDL/DML
# would let anyone with API access drop or corrupt the lakehouse tables dbt
# builds. SELECT/WITH (CTEs) covers every legitimate ad-hoc analytics use
# case; anything else is rejected. Enforced twice, from the same function —
# once here at submission (fails fast, before an ECS task is even launched)
# and again inside ingestion/query_runner.py right before execution (in
# case a caller reaches the runner some other way than through this API).
_ALLOWED_LEADING_KEYWORDS = ("select", "with")
_SQL_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)


def _first_keyword(sql: str) -> str:
    stripped = _SQL_COMMENT_RE.sub(" ", sql).strip()
    match = re.match(r"[A-Za-z]+", stripped)
    return match.group(0).lower() if match else ""


def validate_read_only_sql(sql: str) -> None:
    """Raises ValueError with a caller-safe message if `sql` isn't a single
    read-only SELECT/WITH statement."""
    keyword = _first_keyword(sql)
    if keyword not in _ALLOWED_LEADING_KEYWORDS:
        display = repr(keyword) if keyword else "<empty>"
        raise ValueError(
            f"only SELECT/WITH queries are allowed via the query API, got a statement starting with {display}"
        )
    if ";" in sql.rstrip().rstrip(";"):
        # A second statement after the first `;` would run too — DuckDB
        # executes a semicolon-separated batch as one `execute()` call (see
        # ARCHITECTURE.md §3.17), which is exactly what would let a second,
        # unvalidated statement slip past the check above.
        raise ValueError("multiple statements are not allowed via the query API")


def query_key(job_id: str) -> str:
    return f"queries/{job_id}/query.sql"


def status_key(job_id: str) -> str:
    return f"queries/{job_id}/status.json"


def result_key(job_id: str) -> str:
    return f"queries/{job_id}/result.parquet"


_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def is_valid_job_id(job_id: str) -> bool:
    # Job ids flow straight into an S3 key (query_key/status_key/result_key
    # above) and, for submission, into an ECS RunTask environment override —
    # validating the exact shape new_job_id() produces up front means a
    # malformed path segment from a client can never reach either.
    return bool(_JOB_ID_RE.match(job_id))


def ecs_run_task_kwargs(
    *,
    cluster: str,
    task_definition: str,
    subnets: list,
    security_groups: list,
    container_name: str,
    job_id: str,
    query_s3_key: str,
    assign_public_ip: str = "ENABLED",
) -> dict:
    """kwargs for boto3's ecs.run_task(**kwargs) — one place defining the
    override shape, used identically by the Lambda submit handler and by
    local_api.py, so the two never drift on how a query task is launched.
    """
    return {
        "cluster": cluster,
        "taskDefinition": task_definition,
        "launchType": "FARGATE",
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": assign_public_ip,
            }
        },
        "overrides": {
            "containerOverrides": [
                {
                    "name": container_name,
                    "environment": [
                        {"name": "MODE", "value": "query"},
                        {"name": "JOB_ID", "value": job_id},
                        {"name": "QUERY_S3_KEY", "value": query_s3_key},
                    ],
                }
            ]
        },
    }
