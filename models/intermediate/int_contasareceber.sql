with contas_receber as (
    select * from {{ ref('stg_contasareceber') }}
),

final as (
    select
        id_filial,
        id_cliente,
        id_cobranca,
        id_cobranca_original,
        id_vendedor,
        numero_prestacao,
        duplicata,
        data_vencimento,
        data_pagamento,
        data_emissao,
        valor_original,
        valor_pago
    from contas_receber
)

select * from final