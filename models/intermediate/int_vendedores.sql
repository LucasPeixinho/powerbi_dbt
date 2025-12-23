with vendedores as (
    select * from {{ ref('stg_vendedores') }}
),

final as (
    select
        id_vendedor,
        nome_vendedor,
        id_supervisor,
        id_vendedor || ' - ' || nome_vendedor as vendedor
    from vendedores
)

select * from final