{{ config(materialized='table') }}

with stg_pharmacies as (
    select * from {{ ref('stg_pharmacies') }}
)

select
    pharmacy_id,
    pharmacy_name,
    pharmacy_address,
    city,
    registration_date
from stg_pharmacies