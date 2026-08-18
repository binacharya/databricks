with source as (
    select * from {{ ref('products') }}
),

renamed as (
    select
       *
    from source
)

select * from renamed