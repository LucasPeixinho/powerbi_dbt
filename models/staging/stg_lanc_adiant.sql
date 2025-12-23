with source as (
    select * from {{ source('cedep', 'pclancadiantfornec') }}
),

final as (
    select
        RECNUMADIANTAMENTO as id_lancamento_adiantamento,
        RECNUMPAGTO as id_lancamento_pagamento,
        VALOR as valor_original,
        DTLANC as data_lancamento,
        DTESTORNO as data_estorno
    from 
        source
    where
        DTESTORNO is null
)

select * from final