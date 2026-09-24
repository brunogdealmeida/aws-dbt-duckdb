{#
  Estratégia incremental customizada do dbt-duckdb que mantém uma dimensão
  SCD Tipo 2 (Slowly Changing Dimension) em cima de uma tabela Iceberg.

  O model alimenta essa estratégia com um *snapshot completo* do estado
  atual — uma linha por chave natural, já com as colunas de controle
  (`scd_hash`, `valid_from`, `valid_to`, `is_current`). A estratégia compara
  esse snapshot com as versões correntes que já estão na dimensão e:

    1. FECHA as versões correntes cuja chave natural sumiu do snapshot ou
       cujo `scd_hash` mudou (`valid_to` = timestamp do lote, `is_current`
       = false);
    2. INSERE uma versão nova pra toda chave natural que ficou sem versão
       corrente depois do passo 1 — ou seja, as que mudaram (acabaram de
       ser fechadas) e as que nunca existiram.

  Chaves cujo `scd_hash` não mudou não são tocadas por nenhum dos dois
  statements: a versão corrente continua aberta e o histórico não cresce à
  toa.

  Por que dois statements e não um MERGE: uma chave que mudou precisa de um
  UPDATE (fechar a versão antiga) *e* de um INSERT (abrir a nova) para a
  mesma linha de origem, o que um único MERGE não consegue expressar — e o
  Iceberg do DuckDB ainda por cima só aceita uma ação de UPDATE/DELETE por
  MERGE (ver §3.17 do ARCHITECTURE.md). Confirmado direto contra o bucket
  real do S3 Tables que as duas formas usadas aqui funcionam no Iceberg:
  `UPDATE ... WHERE <chave> IN (subquery)` e `INSERT ... SELECT` com
  anti-join contra a própria tabela alvo. Os dois statements separados por
  `;` rodam numa única chamada `execute()`, que é como o dbt-duckdb executa
  o SQL compilado de uma materialização.

  Use via `{{ config(materialized='incremental', incremental_strategy='scd2_merge', unique_key='<chave natural>') }}`
  (ver dbt/models/silver/dim_client_portfolio.sql).

  Configs opcionais do model, caso as colunas de controle tenham outros
  nomes: `scd2_hash_column` (default 'scd_hash'), `scd2_valid_from_column`
  ('valid_from'), `scd2_valid_to_column` ('valid_to') e
  `scd2_is_current_column` ('is_current').
#}
{#
  Assim como em incremental_strategy_cdc_merge.sql, o dbt-core resolve o
  macro da estratégia pelo nome sem prefixo via `adapter.dispatch`, então
  esse repassador precisa existir além da implementação `duckdb__`.
#}
{% macro get_incremental_scd2_merge_sql(args_dict) %}
    {{ return(adapter.dispatch('get_incremental_scd2_merge_sql')(args_dict)) }}
{% endmacro %}

{% macro duckdb__get_incremental_scd2_merge_sql(args_dict) %}
    {%- set target = args_dict['target_relation'] -%}
    {%- set source = args_dict['temp_relation'] -%}
    {%- set unique_key = args_dict['unique_key'] -%}

    {%- set hash_col = config.get('scd2_hash_column', 'scd_hash') -%}
    {%- set valid_from_col = config.get('scd2_valid_from_column', 'valid_from') -%}
    {%- set valid_to_col = config.get('scd2_valid_to_column', 'valid_to') -%}
    {%- set is_current_col = config.get('scd2_is_current_column', 'is_current') -%}

    {%- set insert_columns = [] -%}
    {%- for col in args_dict['dest_columns'] -%}
        {%- do insert_columns.append(col.name) -%}
    {%- endfor -%}

    {#-
      O timestamp do lote sai do próprio snapshot (`max(valid_from)`, igual
      em todas as linhas da origem) em vez de um `current_timestamp` novo:
      assim o `valid_to` da versão fechada é exatamente o `valid_from` da
      versão nova, sem buraco nem sobreposição na linha do tempo. O
      coalesce cobre o snapshot vazio, em que o max seria NULL e deixaria
      versões fechadas sem data de fim.
    -#}
    {%- set batch_ts -%}
        coalesce(
            (select max({{ valid_from_col }}) from {{ source }}),
            cast(current_timestamp as timestamp)
        )
    {%- endset -%}

    update {{ target }}
    set
        {{ valid_to_col }} = {{ batch_ts }},
        {{ is_current_col }} = false
    where {{ is_current_col }}
      and {{ unique_key }} in (
          select t.{{ unique_key }}
          from {{ target }} t
          left join {{ source }} s
            on s.{{ unique_key }} = t.{{ unique_key }}
          where t.{{ is_current_col }}
            and (
                s.{{ unique_key }} is null
                or s.{{ hash_col }} is distinct from t.{{ hash_col }}
            )
      );

    {#-
      Depois do UPDATE acima, toda chave que mudou ficou sem versão
      corrente — então este anti-join pega, de uma vez só, as chaves novas
      e as que acabaram de ser fechadas, e ignora as inalteradas (que
      seguem com a versão corrente aberta).
    -#}
    insert into {{ target }} ({{ insert_columns | join(', ') }})
    select {% for col_name in insert_columns %}s.{{ col_name }}{{ ", " if not loop.last }}{% endfor %}
    from {{ source }} s
    left join {{ target }} t
      on t.{{ unique_key }} = s.{{ unique_key }}
     and t.{{ is_current_col }}
    where t.{{ unique_key }} is null;
{% endmacro %}
