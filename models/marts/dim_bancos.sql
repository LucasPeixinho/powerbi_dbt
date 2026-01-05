with bancos as (
    select * from {{ ref('int_bancos') }}
),

final as (
    select
        id_banco,
        nome_banco
    from 
        bancos
)

select * from final