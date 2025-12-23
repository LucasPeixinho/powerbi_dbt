with lancamentos_adiantamento as (
    select * from {{ ref('stg_lanc_adiant') }}
),

lancamentos as (
    select * from {{ ref('stg_contasapagar') }}
),

final as (
    select
        ba.id_lancamento,
        la.numero_nota,
        la.id_conta,
        la.id_fornecedor,
        la.id_filial,
        la.data_emissao,
        la.data_lancamento,
        la.data_vencimento,
        ba.data_pagamento,
        la.data_competencia,
        la.duplicata,
        ((ba.valor_original + coalesce(ba.valor_variacao_cambial, 0)) * (-1)) as valor_original,
        ((ba.valor_pago + coalesce(ba.valor_variacao_cambial, 0)) * (-1)) as valor_pago,
        la.tipo_lancamento
    from  
        lancamentos_adiantamento a
    left join
        lancamentos la
        on a.id_lancamento_adiantamento = la.id_lancamento
    left join
        lancamentos ba
        on a.id_lancamento_pagamento = ba. id_lancamento
    where
        ba.data_pagamento is not null
        and ba.data_estorno_baixa is null
)

select * from final
