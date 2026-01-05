with bancos as (
    select * from {{ ref('stg_bancos') }}
),

final as (
    select
        id_banco,
        nome_banco
    from 
        bancos
)

select * from final