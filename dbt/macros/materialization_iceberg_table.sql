{#
  A materialização `table` nativa do dbt-duckdb cria uma relação
  intermediária e troca o nome via rename (ver o table.sql dela), e a
  materialização `incremental` faz o mesmo em toda run depois da primeira.
  A integração do DuckDB com catálogo Iceberg ainda não suporta esse
  rename ("Not implemented Error: Alter Schema Entry" — confirmado
  reproduzindo direto contra o bucket real do S3 Tables). `DROP TABLE` +
  `CREATE TABLE ... AS` simples funcionam bem contra ele, então essa
  materialização faz um drop-and-recreate completo em toda run em vez de
  um swap. Usada só no target `prod` (ver dbt_project.yml); o `dev` usa a
  materialização `table` padrão contra um arquivo DuckDB local, que não
  tem essa limitação.
#}
{% materialization iceberg_table, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is not none %}
    {#-- adapter.drop_relation() emite DROP ... CASCADE, não suportado em
         tabelas Iceberg; um DROP TABLE simples funciona bem. Commitado
         separadamente antes do CREATE abaixo: versões mais novas da
         extensão duckdb-iceberg rejeitam criar uma tabela com o mesmo nome
         de uma apagada antes, ainda na mesma transação aberta ("Cannot
         create table deleted within a transaction"). #}
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
