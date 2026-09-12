{#
  dbt-duckdb's built-in `table` materialization creates an intermediate
  relation and swaps it in via a rename (see its table.sql), and its
  `incremental` materialization does the same on every run after the first.
  DuckDB's Iceberg catalog integration doesn't support that rename yet
  ("Not implemented Error: Alter Schema Entry" — confirmed by reproducing it
  directly against the real S3 Tables bucket). Plain `DROP TABLE` +
  `CREATE TABLE ... AS` both work fine against it, so this materialization
  does a full drop-and-recreate every run instead of a swap. Used only for
  the `prod` target (see dbt_project.yml); `dev` uses the standard `table`
  materialization against a local DuckDB file, which doesn't have this
  limitation.
#}
{% materialization iceberg_table, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is not none %}
    {#-- adapter.drop_relation() issues DROP ... CASCADE, unsupported on
         Iceberg tables; a plain DROP TABLE works fine. Committed on its own
         before the CREATE below: newer duckdb-iceberg extension versions
         reject creating a table with the same name deleted earlier in the
         same still-open transaction ("Cannot create table deleted within a
         transaction"). #}
    {% call statement('drop_existing') -%}
      drop table if exists {{ target_relation }}
    {%- endcall %}
    {{ adapter.commit() }}
  {% endif %}

  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, compiled_code) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
