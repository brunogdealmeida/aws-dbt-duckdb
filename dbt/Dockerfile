# Build context MUST be the repository root, e.g.:
#   docker build -f dbt/Dockerfile -t lakehouse .
FROM python:3.12-slim

ARG DBT_CORE_VERSION=1.11.8
ARG DBT_DUCKDB_VERSION=1.10.0
ARG DUCKDB_VERSION=1.4.0

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

RUN dbt parse

ENTRYPOINT ["python", "ingestion/entrypoint.py"]
