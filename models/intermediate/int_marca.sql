with marcas as (
    select * from {{ ref('stg_marca') }}
),

final as (
    select
        id_marca,
        nome_marca
    from marcas
)

select * from final