{{ config(materialized='table') }}

with 
stg_sales as (
    select * from {{ ref('stg_sales') }}
),

stg_patients as (
    select * from {{ ref('stg_patients') }}
),

stg_medicines as (
    select * from {{ ref('stg_medicines') }}
)

select
    s.sale_id,
    s.patient_id,
    s.medicine_id,
    s.pharmacy_id,
    s.quantity,
    m.price,
    s.quantity * m.price as total_amount,
    s.sale_status,
    s.sale_date 
from stg_sales as s

join stg_patients as p 
    on p.patient_id = s.patient_id
join stg_medicines as m 
    on m.medicine_id = s.medicine_id