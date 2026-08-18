with source as (
    select * from {{ ref('orders') }}
),

renamed as (
    select
       *
    from source
)

select * from renamed
