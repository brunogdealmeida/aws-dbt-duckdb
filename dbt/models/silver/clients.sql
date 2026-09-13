{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='cdc_merge'
) }}

{% if is_incremental() %}

select
    cast(customer_id as bigint) as customer_id,
    trim(name) as name,
    lower(trim(email)) as email,
    upper(trim(country)) as country,
    cast(signup_date as date) as signup_date,
    lower(trim(segment)) as segment,
    upper(trim(_cdc_op)) as _cdc_op
from {{ source('bronze', 'clients_cdc') }}
where customer_id is not null

{% else %}

select
    customer_id,
    name,
    email,
    country,
    signup_date,
    segment
from {{ ref('stg_clients') }}

{% endif %}
