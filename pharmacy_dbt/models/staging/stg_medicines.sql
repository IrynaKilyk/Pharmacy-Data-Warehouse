with source as (
    select * from {{ source('raw_pharmacy', 'medicines') }}
)

select
    medicine_id,
    regexp_replace(medicine_name, '\s*\d+\s*mg$', '') as medicine_name,
    substring(medicine_name from '(\d+\s*mg)$') as dosage,
    cast(case when price < 0 then 0 else price end as numeric(10, 2)) as price,
    is_active
from source