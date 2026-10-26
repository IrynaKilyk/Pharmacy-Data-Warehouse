with source as (
    select * from {{ source('raw_pharmacy', 'reviews') }}
)

select
    review_id,
    patient_id,
    medicine_id,
    rating,
    coalesce(nullif(trim(comment), ''), 'No comments') as user_comment,
    review_date

from source