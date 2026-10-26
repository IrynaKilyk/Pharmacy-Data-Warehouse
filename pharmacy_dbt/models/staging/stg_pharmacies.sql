with source as (
    select * from {{ source('raw_pharmacy', 'pharmacies') }}
),

deduplicated as (
    select*,
        row_number() over (partition by pharmacy_id order by registration_date asc) as rn
    from source
)

select
    pharmacy_id,
    pharmacy_name,
    coalesce(nullif(trim(address), ''), 'Unknown') as pharmacy_address,
    city,
    registration_date
from deduplicated
where rn = 1