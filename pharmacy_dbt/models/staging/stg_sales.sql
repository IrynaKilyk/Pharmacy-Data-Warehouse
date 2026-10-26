with source as (
    select * from {{ source('raw_pharmacy', 'sales') }}
)

select
    sale_id,
    patient_id,
    medicine_id,
    pharmacy_id,
    sale_date,
    quantity,
    status as sale_status,
cast(sale_date as timestamp) as created_at
from source