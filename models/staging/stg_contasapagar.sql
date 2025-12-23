with source as (
    select * from {{ source('cedep', 'pclanc') }}
),

final as (
    select
        RECNUM as id_lancamento,
        NUMNOTA as numero_nota,
        CODCONTA as id_conta,
        CODFORNEC as id_fornecedor,
        CODFILIAL as id_filial,
        DTEMISSAO as data_emissao,
        DTLANC as data_lancamento,
        DTVENC as data_vencimento,
        DTPAGTO as data_pagamento,
        DTESTORNOBAIXA as data_estorno_baixa,
        DTCOMPETENCIA as data_competencia,
        DUPLIC as duplicata,
        VALOR as valor_original,
        VPAGO as valor_pago,
        VLVARIACAOCAMBIAL as valor_variacao_cambial,
        TIPOLANC as tipo_lancamento
    from 
        source
)

select * from final