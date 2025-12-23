with contas_receber as (
    select * from {{ ref('int_contasareceber') }}
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
        valor_pago,
        case
            when data_pagamento is not null and data_pagamento <= trunc(sysdate) then 'PAGO'
            when data_vencimento >= trunc(sysdate) then 'EM DIA'
            when data_vencimento < trunc(sysdate) and data_vencimento > trunc(sysdate) - 10 then 'ATRASO'
            when data_vencimento <= trunc(sysdate) - 10 and data_vencimento >= add_months(trunc(sysdate) - 10, -6) then 'INADIMPLENTE'
            when data_vencimento <add_months(trunc(sysdate) -10, -6) then 'PERDIDO'
            else 'INDEFINIDO'
            end as situacao
    from contas_receber
    where id_cobranca not in ('DEVP', 'DEVT', 'BNF', 'BNFT', 'BNFR', 'BNTR', 'BNRP', 'CRED', 'DESD')
    and {{ filtro_periodo('data_vencimento') }}
)

select * from final