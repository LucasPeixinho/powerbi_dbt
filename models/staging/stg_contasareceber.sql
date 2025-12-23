with source as (
    select * from {{ source('cedep', 'pcprest') }}
),

renamed as (
    select
        CODFILIAL as id_filial,
        CODCLI as id_cliente,
        CODCOB as id_cobranca,
        CODCOBORIG as id_cobranca_original,
        CODUSUR as id_vendedor,
        PREST as numero_prestacao,
        DUPLIC as duplicata,
        DTVENC as data_vencimento,
        DTPAG as data_pagamento,
        DTEMISSAO as data_emissao,
        VALOR as valor_original,
        VPAGO as valor_pago
    from source
)

select * from renamed


    