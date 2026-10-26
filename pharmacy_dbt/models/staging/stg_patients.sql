with source as (
    select * from {{ source('raw_pharmacy', 'patients') }}
)

select
    patient_id,
    patient_name,
    birth_date,
    city
from source