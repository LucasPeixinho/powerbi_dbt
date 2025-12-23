with cobrancas as (
    select * from {{ ref('stg_cobrancas') }}
),

final as (
    select
        id_cobranca,
        nome_cobranca
    from 
        cobrancas
)

select * from final