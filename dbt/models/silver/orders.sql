{{ config(materialized='table') }}

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(order_date as date) as order_date,
    cast(amount as decimal(18,2)) as amount,
    lower(trim(status)) as status
from {{ source('bronze', 'orders') }}
where order_id is not null
