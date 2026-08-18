-- No future order_date
select
    order_id,
    order_date
from {{ ref('fct_orders') }}
where order_date > current_date()
