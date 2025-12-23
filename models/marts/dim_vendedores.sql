with vendedores as (
    select * from {{ ref('int_vendedores') }}
),

final as (
    select
        id_vendedor, 
        nome_vendedor, 
        id_supervisor,
        vendedor
    from vendedores
)

select * from final