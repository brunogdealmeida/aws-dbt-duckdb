{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='scd2_merge'
) }}

{#
  Dimensão SCD Tipo 2 da hierarquia de carteira de clientes:
  cliente -> carteira -> gerente -> região/tier.

  A fonte `bronze.portfolios` é um *snapshot completo* do estado atual (uma
  linha por cliente, como um extrato diário de CRM — ver
  ingestion/simulate_portfolio_changes.py pra simular mudanças). Este model
  só produz o snapshot já com as colunas de controle; quem compara com o
  histórico e decide o que fechar/abrir é a estratégia `scd2_merge`
  (dbt/macros/incremental_strategy_scd2_merge.sql).

  Uma linha por versão: quando um cliente troca de carteira (ou a carteira
  troca de gerente/região/tier), a versão vigente é fechada com
  `valid_to` = o instante da carga e uma nova é aberta. Modelos analíticos
  fazem o join *point-in-time* — casando cada fato com a versão vigente na
  data do fato — em vez de atribuir todo o histórico à carteira atual (ver
  models/gold/fct_portfolio_revenue.sql).
#}

with snapshot as (

    select
        cast(customer_id as bigint) as customer_id,
        cast(portfolio_id as bigint) as portfolio_id,
        trim(portfolio_name) as portfolio_name,
        cast(manager_id as bigint) as manager_id,
        trim(manager_name) as manager_name,
        upper(trim(region)) as region,
        lower(trim(tier)) as tier
    from {{ source('bronze', 'portfolios') }}
    where customer_id is not null

),

deduped as (

    {#
      SCD2 exige exatamente uma linha de entrada por chave natural: um
      snapshot malformado com o mesmo cliente duas vezes abriria duas
      versões correntes pra ele, quebrando todo join point-in-time daí pra
      frente. O teste `unique` (where is_current) em schema.yml cobre isso,
      mas aqui já garantimos na entrada.
    #}
    select * from snapshot
    qualify row_number() over (partition by customer_id order by portfolio_id) = 1

),

versioned as (

    select
        *,

        {#
          Hash só das colunas *rastreadas*: é ele que decide se houve
          mudança. Colunas derivadas/descritivas que acompanham a chave
          (portfolio_name, manager_name) ficam de fora de propósito — um
          rename de carteira não deve gerar uma versão nova.
        #}
        md5(concat_ws('|',
            cast(portfolio_id as varchar),
            cast(manager_id as varchar),
            region,
            tier
        )) as scd_hash,

        {% if is_incremental() %}

        {#- Mudanças detectadas depois da carga inicial valem a partir de agora. -#}
        cast(current_timestamp as timestamp) as valid_from

        {% else %}

        {#
          Carga inicial: a primeira versão vale desde o "início dos tempos",
          não desde o instante do primeiro `dbt build`. Sem isso, o join
          point-in-time do gold não casaria com nenhum pedido histórico
          (todos anteriores à carga) e a camada gold sairia vazia.
        #}
        cast('1900-01-01' as timestamp) as valid_from

        {% endif %}

    from deduped

)

select
    {#
      Chave substituta da *versão* (não do cliente): é a PK da dimensão e o
      que um fato guardaria pra congelar a atribuição histórica.
    #}
    md5(concat_ws('|',
        cast(customer_id as varchar),
        scd_hash,
        cast(valid_from as varchar)
    )) as portfolio_version_key,

    customer_id,
    portfolio_id,
    portfolio_name,
    manager_id,
    manager_name,
    region,
    tier,
    scd_hash,
    valid_from,

    {#
      timestamp (sem timezone) de propósito nas duas pontas: o join
      point-in-time do gold compara com `order_date`, que é DATE — misturar
      com timestamptz deixaria o resultado dependente do fuso da sessão.
    #}
    cast(null as timestamp) as valid_to,
    true as is_current

from versioned
