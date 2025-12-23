with movimentos as (
    select * from {{ ref('stg_movimentacao') }}
    where tipo_movimento = 'SAIDA'
),

notas_saida as (
    select * from {{ ref('stg_notas_saida') }}
),

produtos as (
    select * from {{ ref('stg_produtos') }}
),

vendas_completas as (
    select
        m.id_transacao_venda,
        m.id_produto,
        m.id_filial,
        m.data_movimento,
        m.codigo_operacao,
        
        -- Produto e Fornecedor
        p.id_fornecedor,
        p.descricao_produto,
        p.id_departamento,
        p.id_secao,
        p.id_marca,
        
        -- Cliente e Vendedor
        ns.id_cliente,
        ns.id_vendedor,
        ns.id_supervisor,
        
        -- Quantidades
        m.quantidade,
        
        -- Valores
        m.preco_unitario,
        m.custo_financeiro_movimento as custo_unitario,
        m.quantidade * m.preco_unitario as receita_bruta,
        m.quantidade * m.custo_financeiro_movimento as custo_total,
        
        -- Descontos e Impostos
        coalesce(m.valor_desconto, 0) as valor_desconto,
        coalesce(m.valor_st, 0) as valor_st,
        coalesce(m.valor_ipi, 0) as valor_ipi,
        --coalesce(m.valor_icms_st, 0) as valor_icms_st,
        
        -- Despesas da nota (rateadas proporcionalmente)
        case 
            when ns.valor_total_nota > 0 
            then coalesce(ns.valor_frete, 0) * (m.quantidade * m.preco_unitario) / ns.valor_total_nota
            else 0 
        end as valor_frete_rateado,
        
        case 
            when ns.valor_total_nota > 0 
            then coalesce(ns.valor_outras_despesas, 0) * (m.quantidade * m.preco_unitario) / ns.valor_total_nota
            else 0 
        end as valor_outras_despesas_rateado
        
    from movimentos m
    left join notas_saida ns 
        on m.id_transacao_venda = ns.id_transacao_venda
    left join produtos p 
        on m.id_produto = p.id_produto
)

select * from vendas_completas