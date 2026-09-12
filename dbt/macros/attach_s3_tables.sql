{#
  Attaches the S3 Tables bucket to the running DuckDB connection as the
  `s3_tables` catalog, using DuckDB's native Iceberg REST catalog support
  (ENDPOINT_TYPE s3_tables). Models write into it directly via
  `{{ config(database='s3_tables', schema=<namespace>) }}` — see the
  `+database`/`+schema` config on the `silver` folder in dbt_project.yml.

  Requires DuckDB >= 1.4 with the httpfs/aws/iceberg extensions loaded
  (see dbt/profiles.yml `prod` target) and IAM permissions on the S3 Tables
  bucket (granted to the ECS task role in infra/main.tf). Credentials are
  picked up automatically from the ECS task role via the `credential_chain`
  provider — no keys are embedded anywhere.

  This runs only against the `prod` target so that `dbt parse`/`dbt compile`
  in CI (target `dev`, no AWS access) and any local `dbt run --target dev`
  work without AWS credentials.

  Iceberg write support (CREATE TABLE / INSERT INTO) via DuckDB's REST
  catalogs is a fast-moving area of DuckDB itself. Validate with a real
  `dbt run --target prod` against dev infra before relying on this in
  production, and watch for `CREATE OR REPLACE TABLE AS` issues on some
  catalog backends — switch the silver models to `+materialized: incremental`
  if plain `table` materialization fails to replace an existing Iceberg table.
#}
{% macro attach_s3_tables() %}
  {% if target.name == 'prod' %}
    {% set account_id = env_var('AWS_ACCOUNT_ID') %}
    {% set region = env_var('AWS_REGION', 'us-east-1') %}
    {% set bucket = env_var('S3_TABLE_BUCKET') %}

    {% set bucket_arn = 'arn:aws:s3tables:' ~ region ~ ':' ~ account_id ~ ':bucket/' ~ bucket %}

    {% do run_query("
      CREATE OR REPLACE SECRET s3_tables_secret (
          TYPE s3,
          PROVIDER credential_chain,
          REGION '" ~ region ~ "'
      );
    ") %}

    {% do run_query("
      ATTACH IF NOT EXISTS '" ~ bucket_arn ~ "' AS s3_tables (
          TYPE iceberg,
          ENDPOINT_TYPE s3_tables,
          SECRET s3_tables_secret
      );
    ") %}
  {% endif %}
{% endmacro %}
