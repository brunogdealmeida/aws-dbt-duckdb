{#
  Custom dbt-duckdb incremental strategy for applying a CDC batch (rows
  tagged with a `_cdc_op` column: 'I' insert, 'U' update, 'D' delete) to an
  existing Iceberg table.

  Confirmed directly against the real S3 Tables bucket: DuckDB's Iceberg
  MERGE INTO only supports a single UPDATE/DELETE action per statement
  ("Not implemented Error: MERGE INTO with Iceberg only supports a single
  UPDATE/DELETE action currently") — a single MERGE combining a conditional
  DELETE and a conditional UPDATE (as dbt-duckdb's built-in `merge` strategy
  generates) fails. Splitting into two statements — a plain DELETE, then a
  MERGE with only UPDATE+INSERT — works, and DuckDB executes both when
  passed to one `execute()` call as a semicolon-separated batch, which is
  how dbt-duckdb runs a materialization's compiled SQL.

  Use via `{{ config(materialized='incremental', incremental_strategy='cdc_merge', unique_key=...) }}`
  on a model whose compiled SELECT includes a `_cdc_op` column with values
  'I'/'U'/'D' (see dbt/models/silver/{orders,clients,inventory}.sql).
#}
{#
  dbt-core's incremental materialization looks up a strategy's SQL macro by
  its plain (unprefixed) name via `adapter.dispatch`, which only works if
  that plain name exists as a macro that itself calls dispatch — mirroring
  how dbt-core's own built-in strategies (get_incremental_merge_sql, etc.)
  are defined. Without this, dbt errors with "could not find an incremental
  strategy macro" even though the duckdb__-prefixed implementation exists.
#}
{% macro get_incremental_cdc_merge_sql(args_dict) %}
    {{ return(adapter.dispatch('get_incremental_cdc_merge_sql')(args_dict)) }}
{% endmacro %}

{% macro duckdb__get_incremental_cdc_merge_sql(args_dict) %}
    {%- set target = args_dict['target_relation'] -%}
    {%- set source = args_dict['temp_relation'] -%}
    {%- set unique_key = args_dict['unique_key'] -%}

    delete from {{ target }}
    where {{ unique_key }} in (
        select {{ unique_key }} from {{ source }} where _cdc_op = 'D'
    );

    merge into {{ target }} as DBT_INTERNAL_DEST
    using (select * exclude (_cdc_op) from {{ source }} where _cdc_op != 'D') as DBT_INTERNAL_SOURCE
    on (DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }})
    when matched then
        update set *
    when not matched then
        insert *;
{% endmacro %}
