{#
  Receita mensal por carteira/gerente/região, com atribuição *point-in-time*
  contra a dimensão SCD Tipo 2 models/silver/dim_client_portfolio.sql.

  O ponto todo de manter a dimensão como SCD2 está no join abaixo: cada
  pedido é atribuído à carteira/gerente que era dono do cliente **na data do
  pedido**, não à carteira atual. Um cliente que trocou de carteira em
  julho deixa o histórico até junho com o gerente antigo e só o que vier
  depois com o novo — que é como comissionamento, metas e série histórica
  precisam ser lidos. Um join simples pela versão corrente (`is_current`)
  reescreveria o passado a cada mudança de carteira.

  Grão: uma linha por (carteira-na-época × mês do pedido).
#}

with orders as (

    select
        customer_id,
        order_date,
        amount,
        status
    from {{ ref('stg_orders') }}
    where customer_id is not null
      and order_date is not null

),

attributed as (

    select
        o.customer_id,
        o.order_date,
        o.amount,
        o.status,
        d.portfolio_id,
        d.portfolio_name,
        d.manager_id,
        d.manager_name,
        d.region,
        d.tier
    from orders o
    join {{ ref('dim_client_portfolio') }} d
      on d.customer_id = o.customer_id

     {#
       O join point-in-time propriamente dito: a versão cujo intervalo
       [valid_from, valid_to) contém a data do pedido. `valid_to is null`
       é a versão ainda aberta. Intervalo fechado-aberto pra que um pedido
       feito exatamente no instante da troca caia só na versão nova.
     #}
     and cast(o.order_date as timestamp) >= d.valid_from
     and (d.valid_to is null or cast(o.order_date as timestamp) < d.valid_to)

),

aggregated as (

    select
        region,
        tier,
        portfolio_id,
        portfolio_name,
        manager_id,
        manager_name,
        cast(date_trunc('month', order_date) as date) as order_month,

        count(*) as order_count,
        count(*) filter (where status = 'paid') as paid_order_count,
        count(distinct customer_id) as active_customers,

        {#
          `amount` só vira receita quando o pedido foi pago; pendente e
          estornado ficam em colunas próprias em vez de somados junto ou
          descartados — quem consome decide o que considerar.
        #}
        sum(amount) filter (where status = 'paid') as paid_revenue,
        sum(amount) filter (where status = 'pending') as pending_amount,
        sum(amount) filter (where status = 'refunded') as refunded_amount,
        round(avg(amount) filter (where status = 'paid'), 2) as avg_paid_order_value

    from attributed
    group by all

)

select
    *,
    cast(current_timestamp as timestamp) as generated_at
from aggregated
