{{ config(materialized='table') }}

with stg_patients as (
    select * from {{ ref('stg_patients') }}
)

select
    patient_id,
    patient_name,
    birth_date,
    city
from stg_patients