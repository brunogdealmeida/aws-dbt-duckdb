{#
  Estratégia incremental customizada do dbt-duckdb pra aplicar um lote de
  CDC (linhas marcadas com uma coluna `_cdc_op`: 'I' insert, 'U' update,
  'D' delete) numa tabela Iceberg existente.

  Confirmado direto contra o bucket real do S3 Tables: o MERGE INTO do
  Iceberg no DuckDB só suporta uma ação de UPDATE/DELETE por statement
  ("Not implemented Error: MERGE INTO with Iceberg only supports a single
  UPDATE/DELETE action currently") — um único MERGE combinando um DELETE
  condicional e um UPDATE condicional (como a estratégia `merge` nativa do
  dbt-duckdb gera) falha. Dividir em dois statements — um DELETE simples,
  depois um MERGE só com UPDATE+INSERT — funciona, e o DuckDB executa os
  dois quando passados numa única chamada `execute()` como lote separado
  por `;`, que é como o dbt-duckdb roda o SQL compilado de uma
  materialização.

  Use via `{{ config(materialized='incremental', incremental_strategy='cdc_merge', unique_key=...) }}`
  num model cujo SELECT compilado inclua uma coluna `_cdc_op` com valores
  'I'/'U'/'D' (ver dbt/models/silver/stg_{orders,clients,inventory}.sql).

  Config opcional do model `cdc_merge_preserve_on_update` (lista de nomes
  de coluna, default []): colunas que ficam de fora da branch de UPDATE do
  merge, mantendo o valor já existente no alvo em vez de sobrescrever com
  o da origem — pra colunas de auditoria como `ingestion_time`, que devem
  registrar quando a linha foi carregada pela primeira vez, não a última
  vez que um lote de CDC tocou nela. Sem isso, `update set *` (a
  abordagem ingênua) sobrescreve toda coluna vinda da origem em todo
  update, inclusive essas colunas de auditoria — confirmado
  empiricamente: depois de uma linha ser atualizada, seu `ingestion_time`
  tinha sido resetado pro timestamp do update em vez de manter a hora
  original de inserção.
#}
{#
  A materialização incremental do dbt-core resolve o macro SQL de uma
  estratégia pelo nome sem prefixo via `adapter.dispatch`, o que só
  funciona se esse nome sem prefixo existir como um macro que ele mesmo
  chama dispatch de novo — espelhando como as próprias estratégias
  nativas do dbt-core (`get_incremental_merge_sql`, etc.) são declaradas.
  Sem isso, o dbt erra com "could not find an incremental strategy macro"
  mesmo com a implementação prefixada com `duckdb__` já existindo.
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
