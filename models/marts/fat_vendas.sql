with vendas as (
    select * from {{ ref('int_vendas_detalhadas') }}
),

metricas_calculadas as (
    select
        -- Chaves para relacionamento com dimensões
        id_transacao_venda,
        data_movimento,
        id_produto,
        id_fornecedor,
        id_filial,
        id_cliente,
        id_vendedor,
        id_departamento,
        id_secao,
        id_marca,
        -- Métricas aditivas (podem ser somadas)
        quantidade,
        receita_bruta,
        valor_desconto,
        receita_bruta - valor_desconto as receita_liquida,
        custo_total,
        valor_frete_rateado,
        valor_outras_despesas_rateado,
        valor_st,
        valor_ipi,
        
        -- Custo total ajustado
        custo_total + valor_frete_rateado + valor_outras_despesas_rateado as custo_total_ajustado,
        
        -- Margem de contribuição
        (receita_bruta - valor_desconto) - 
        (custo_total + valor_frete_rateado + valor_outras_despesas_rateado) as margem_contribuicao,
        
        -- Métricas não aditivas (para cálculo em medidas DAX)
        preco_unitario,
        custo_unitario
        
    from vendas
)

select * from metricas_calculadas