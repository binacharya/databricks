-- Email contains @ (current snapshot rows)
select
    customer_id,
    email
from {{ ref('scd_customers') }}
where dbt_valid_to is null
  and (email not like '%@%' or email is null)
