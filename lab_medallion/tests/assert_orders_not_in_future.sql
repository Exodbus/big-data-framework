-- An order cannot be placed in the future
select order_id, ordered_at
from {{ ref('stg_orders') }}
where ordered_at > now()
