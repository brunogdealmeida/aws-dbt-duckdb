{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='cdc_merge'
) }}

{#
  First run (table doesn't exist yet): full baseline from stg_orders.
  Every run after that: apply the latest CDC batch (insert/update/delete)
  from bronze.orders_cdc via the cdc_merge incremental strategy — see
  dbt/macros/incremental_strategy_cdc_merge.sql. Generate batches with
  ingestion/simulate_cdc.py.
#}

{% if is_incremental() %}

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(order_date as date) as order_date,
    cast(amount as decimal(18,2)) as amount,
    lower(trim(status)) as status,
    upper(trim(_cdc_op)) as _cdc_op
from {{ source('bronze', 'orders_cdc') }}
where order_id is not null

{% else %}

select
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    amount,
    status
from {{ ref('stg_orders') }}

{% endif %}
