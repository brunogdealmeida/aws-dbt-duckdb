# Build context MUST be the repository root, e.g.:
#   docker build -f dbt/Dockerfile -t lakehouse .
FROM python:3.12-slim

ARG DBT_CORE_VERSION=1.11.8
ARG DBT_DUCKDB_VERSION=1.11.0
ARG DUCKDB_VERSION=1.5.5

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app
ENV HOME=/tmp

WORKDIR /app

RUN pip install --no-cache-dir \
    "dbt-core==${DBT_CORE_VERSION}" \
    "dbt-duckdb==${DBT_DUCKDB_VERSION}" \
    "duckdb==${DUCKDB_VERSION}" \
    boto3

COPY dbt/dbt_project.yml dbt/profiles.yml ./
COPY dbt/models ./models
COPY dbt/macros ./macros
COPY ingestion ./ingestion
# Only common.py (pure stdlib) — not the rest of query_service/, which is
# the local-only API/Postgres stack and has its own dependencies. Gives
# ingestion/query_runner.py's MODE=query path (see entrypoint.py) the same
# S3 key layout and read-only SQL guard the Lambda handlers use, without
# duplicating that logic — see the comment at the top of query_runner.py.
COPY query_service/__init__.py query_service/common.py ./query_service/

RUN dbt parse

ENTRYPOINT ["python", "ingestion/entrypoint.py"]
