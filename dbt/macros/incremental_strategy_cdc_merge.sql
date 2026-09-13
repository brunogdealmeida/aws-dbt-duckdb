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
  'I'/'U'/'D' (see dbt/models/silver/stg_{orders,clients,inventory}.sql).

  Optional model config `cdc_merge_preserve_on_update` (list of column
  names, default []): columns to leave untouched by the UPDATE branch of
  the merge, keeping the target's existing value instead of overwriting it
  with the source's — for audit columns like `ingestion_time` that should
  record when a row was first loaded, not the last time a CDC batch
  touched it. Without this, `update set *` (the naive approach) overwrites
  every column from the source on every update, including such audit
  columns — confirmed empirically: after a row was updated, its
  `ingestion_time` had been reset to the update's timestamp instead of
  keeping the original insert time.
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
    {%- set preserve_on_update = config.get('cdc_merge_preserve_on_update', []) -%}
    {%- set update_columns = [] -%}
    {%- for col in args_dict['dest_columns'] -%}
        {%- if col.name not in preserve_on_update -%}
            {%- do update_columns.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}

    delete from {{ target }}
    where {{ unique_key }} in (
        select {{ unique_key }} from {{ source }} where _cdc_op = 'D'
    );

    merge into {{ target }} as DBT_INTERNAL_DEST
    using (select * exclude (_cdc_op) from {{ source }} where _cdc_op != 'D') as DBT_INTERNAL_SOURCE
    on (DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }})
    when matched then
        update set
            {%- for col_name in update_columns %}
            {{ col_name }} = DBT_INTERNAL_SOURCE.{{ col_name }}{{ "," if not loop.last }}
            {%- endfor %}
    when not matched then
        insert *;
{% endmacro %}
