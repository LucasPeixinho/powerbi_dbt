with lancamentos as (
    select * from {{ ref('stg_contasapagar') }}
),

lancamentos_adiantamento as (
    select * from {{ ref('int_lanc_adiant') }}
),

final as (
    select
        id_lancamento,
        numero_nota,
        id_banco,
        id_conta,
        id_fornecedor,
        id_filial,
        data_emissao,
        data_lancamento,
        data_vencimento,
        data_pagamento,
        data_competencia,
        duplicata,
        valor_original,
        valor_pago,
        tipo_lancamento
    from 
        lancamentos

    union all

    select
        id_lancamento,
        numero_nota,
        id_banco,
        id_conta,
        id_fornecedor,
        id_filial,
        data_emissao,
        data_lancamento,
        data_vencimento,
        data_pagamento,
        data_competencia,
        duplicata,
        valor_original,
        valor_pago,
        tipo_lancamento
    from 
        lancamentos_adiantamento
)

select * from final

