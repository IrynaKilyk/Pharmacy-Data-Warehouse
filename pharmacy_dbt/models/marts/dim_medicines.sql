{{ config(materialized='table') }}

with stg_medicines as (
    select * from {{ ref('stg_medicines') }}
)

select
    medicine_id,
    medicine_name,
    dosage,
    price,
    is_active
from stg_medicines