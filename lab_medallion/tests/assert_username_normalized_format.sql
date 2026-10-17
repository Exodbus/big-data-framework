-- username_normalized must be lowercase with no leading/trailing whitespace
select user_id, username, username_normalized
from {{ ref('stg_users') }}
where username_normalized <> lower(username_normalized)
   or username_normalized <> trim(username_normalized)
