{{ config(materialized='table') }}

select
    cast(product_id as bigint) as product_id,
    cast(warehouse_id as integer) as warehouse_id,
    trim(category) as category,
    cast(quantity_on_hand as integer) as quantity_on_hand,
    cast(unit_cost as decimal(18,2)) as unit_cost,
    cast(last_restock_date as date) as last_restock_date,
    lower(trim(status)) as status
from {{ source('bronze', 'inventory') }}
where product_id is not null
