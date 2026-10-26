with source as (
    select * from {{ source('raw_pharmacy', 'prescriptions') }}
)

select
    prescription_id,
    patient_id,
    medicine_id,
    coalesce(nullif(trim(doctor_name), ''), 'Unknown') as doctor_name,
    dosage as intake_instructions,
    prescription_date
from source