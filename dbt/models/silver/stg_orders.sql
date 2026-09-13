{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='cdc_merge'
) }}

{% if is_incremental() %}

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(order_date as date) as order_date,
    cast(amount as decimal(18,2)) as amount,
    lower(trim(status)) as status,
    upper(trim(_cdc_op)) as _cdc_op,
    current_timestamp as ingestion_time,
    current_timestamp as last_updated_time
from {{ source('bronze', 'orders_cdc') }}
where order_id is not null

{% else %}

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(order_date as date) as order_date,
    cast(amount as decimal(18,2)) as amount,
    lower(trim(status)) as status,
    current_timestamp as ingestion_time,
    current_timestamp as last_updated_time
from {{ source('bronze', 'orders') }}
where order_id is not null

{% endif %}
