with contasapagar as (
    select * from {{ ref('int_contasapagar') }}
),

centrocusto as (
    select * from {{ ref('int_rateio_centro_custo') }}
),

contas as (
    select * from {{ ref('dim_contas') }}
),

final as (
    select
        c.id_lancamento,
        c.numero_nota,
        c.id_banco,
        c.id_conta,
        g.id_grupo_conta,
        c.id_fornecedor,
        c.id_filial,
        c.data_emissao,
        c.data_lancamento,
        c.data_vencimento,
        c.data_pagamento,
        c.data_competencia,
        c.duplicata,
        --c.valor_original,
        --c.valor_pago,
        coalesce(cc.id_centro_custo, '99.999') as id_centro_custo,
        coalesce(cc.percentual_rateio, 100) as percentual_rateio,
        (c.valor_original * coalesce(cc.percentual_rateio, 100) / 100) as valor_original,
        (c.valor_pago * coalesce(cc.percentual_rateio, 100) / 100) as valor_pago,
        c.tipo_lancamento
    from contasapagar c
    left join centrocusto cc
        on c.id_lancamento = cc.id_lancamento
    left join contas g
        on g.id_conta = c.id_conta
    where {{ filtro_periodo('c.data_lancamento') }}
    and id_grupo_conta between '200' and '900'
    order by c.id_lancamento, cc.id_centro_custo
)

select * from final