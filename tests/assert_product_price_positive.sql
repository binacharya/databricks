-- Product price must be positive
select
    product_id,
    product_name,
    price
from {{ ref('stg_products') }}
where price <= 0
