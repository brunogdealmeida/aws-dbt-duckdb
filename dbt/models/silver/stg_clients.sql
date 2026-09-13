{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='cdc_merge',
    cdc_merge_preserve_on_update=['ingestion_time']
) }}

{% if is_incremental() %}

select
    cast(customer_id as bigint) as customer_id,
    trim(name) as name,
    lower(trim(email)) as email,
    upper(trim(country)) as country,
    cast(signup_date as date) as signup_date,
    lower(trim(segment)) as segment,
    upper(trim(_cdc_op)) as _cdc_op,
    current_timestamp as ingestion_time,
    current_timestamp as last_updated_time
from {{ source('bronze', 'clients_cdc') }}
where customer_id is not null

{% else %}

select
    cast(customer_id as bigint) as customer_id,
    trim(name) as name,
    lower(trim(email)) as email,
    upper(trim(country)) as country,
    cast(signup_date as date) as signup_date,
    lower(trim(segment)) as segment,
    current_timestamp as ingestion_time,
    current_timestamp as last_updated_time
from {{ source('bronze', 'clients') }}
where customer_id is not null

{% endif %}
