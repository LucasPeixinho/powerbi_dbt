with clientes as (
    select * from {{ ref ('int_clientes') }}
),

final as (
    select
        id_cliente,
        nome_cliente,
        data_primeira_compra,
        data_inicio_atividade,
        tipo_cliente
    from 
        clientes
)

select * from final