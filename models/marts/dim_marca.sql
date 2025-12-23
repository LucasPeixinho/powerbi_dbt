with marcas as (
    select * from {{ ref('int_marca') }}
),

final as (
    select
        id_marca,
        nome_marca
    from marcas
)

select * from final