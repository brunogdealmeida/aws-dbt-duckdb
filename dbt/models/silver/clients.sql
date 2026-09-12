{{ config(materialized='table') }}

select
    cast(customer_id as bigint) as customer_id,
    trim(name) as name,
    lower(trim(email)) as email,
    upper(trim(country)) as country,
    cast(signup_date as date) as signup_date,
    lower(trim(segment)) as segment
from {{ source('bronze', 'clients') }}
where customer_id is not null
