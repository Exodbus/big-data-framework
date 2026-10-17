{{ config(severity='warn') }}

-- The 3-digit ZIP prefix must belong to one of the ranges of the user's state
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip3_ranges') }} r
    where r.state = u.state
      and cast(left(u.zip_code, 3) as integer) between r.zip3_min and r.zip3_max
)
