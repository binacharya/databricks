with source as (
    select * from {{ ref('returns') }}
),

renamed as (
    select
       *
    from source
)

select * from renamed