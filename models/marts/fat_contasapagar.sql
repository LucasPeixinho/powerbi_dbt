with contasapagar as (
    select * from {{ ref('int_contasapagar') }}
),

final as (
    select
        id_lancamento,
        numero_nota,
        duplicata,
        id_conta,
        id_fornecedor,
        id_filial,
        data_emissao,
        data_lancamento,
        data_vencimento,
        data_pagamento,
        data_competencia,
        valor_original,
        valor_pago,
        case
            when tipo_lancamento = 'C' then 'CONFIRMADO'
            when tipo_lancamento = 'P' then 'PROVISIONADO'
            else 'NÃO INFORMADO'
    end as tipo_lancamento
    from contasapagar
    where {{ filtro_periodo('data_emissao') }}
)

select * from final
