{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='cdc_merge',
    cdc_merge_preserve_on_update=['ingestion_time']
) }}

{% if is_incremental() %}

select
    cast(product_id as bigint) as product_id,
    cast(warehouse_id as integer) as warehouse_id,
    trim(category) as category,
    cast(quantity_on_hand as integer) as quantity_on_hand,
    cast(unit_cost as decimal(18,2)) as unit_cost,
    cast(last_restock_date as date) as last_restock_date,
    lower(trim(status)) as status,
    upper(trim(_cdc_op)) as _cdc_op,
    current_timestamp as ingestion_time,
    current_timestamp as last_updated_time
from {{ source('bronze', 'inventory_cdc') }}
where product_id is not null

{% else %}

select
    cast(product_id as bigint) as product_id,
    cast(warehouse_id as integer) as warehouse_id,
    trim(category) as category,
    cast(quantity_on_hand as integer) as quantity_on_hand,
    cast(unit_cost as decimal(18,2)) as unit_cost,
    cast(last_restock_date as date) as last_restock_date,
    lower(trim(status)) as status,
    current_timestamp as ingestion_time,
    current_timestamp as last_updated_time
from {{ source('bronze', 'inventory') }}
where product_id is not null

{% endif %}
