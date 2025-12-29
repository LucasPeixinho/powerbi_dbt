{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'kpi']
) }}

WITH vendas_atual AS (
    SELECT * FROM {{ ref('fat_vendas') }}
    WHERE data_movimento >= TRUNC(SYSDATE, 'MONTH')
      AND data_movimento < ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), 1)
),

vendas_mes_anterior AS (
    SELECT * FROM {{ ref('fat_vendas') }}
    WHERE data_movimento >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)
      AND data_movimento < TRUNC(SYSDATE, 'MONTH')
),

metricas_atual AS (
    SELECT
        -- Período
        MAX(data_movimento) AS data_referencia,
        'Mês Atual' AS periodo,
        
        -- Receitas
        SUM(receita_bruta) AS receita_bruta,
        SUM(receita_liquida) AS receita_liquida,
        SUM(valor_desconto) AS valor_desconto,
        
        -- Custos
        SUM(custo_total) AS custo_produto,
        SUM(valor_frete_rateado) AS custo_frete,
        SUM(valor_outras_despesas_rateado) AS custo_outras_despesas,
        SUM(custo_total_ajustado) AS custo_total_ajustado,
        
        -- Margem
        SUM(margem_contribuicao) AS margem_contribuicao,
        
        -- Volumes
        SUM(quantidade) AS quantidade_vendida,
        COUNT(DISTINCT id_transacao_venda) AS qtd_transacoes,
        COUNT(DISTINCT id_produto) AS qtd_produtos,
        COUNT(DISTINCT id_fornecedor) AS qtd_fornecedores,
        COUNT(DISTINCT id_cliente) AS qtd_clientes,
        COUNT(DISTINCT id_vendedor) AS qtd_vendedores
        
    FROM vendas_atual
),

metricas_anterior AS (
    SELECT
        SUM(receita_liquida) AS receita_liquida_ma,
        SUM(margem_contribuicao) AS margem_contribuicao_ma,
        SUM(custo_total_ajustado) AS custo_total_ajustado_ma,
        SUM(quantidade) AS quantidade_vendida_ma,
        COUNT(DISTINCT id_transacao_venda) AS qtd_transacoes_ma
    FROM vendas_mes_anterior
),

resultado_final AS (
    SELECT
        SYSTIMESTAMP AS data_atualizacao,
        a.*,
        
        -- Métricas calculadas
        CASE WHEN a.receita_liquida > 0 
            THEN (a.margem_contribuicao / a.receita_liquida) * 100 
            ELSE 0 END AS margem_percentual,
            
        CASE WHEN a.custo_total_ajustado > 0 
            THEN (a.margem_contribuicao / a.custo_total_ajustado) * 100 
            ELSE 0 END AS markup_percentual,
            
        CASE WHEN a.receita_bruta > 0 
            THEN (a.valor_desconto / a.receita_bruta) * 100 
            ELSE 0 END AS desconto_percentual,
        
        CASE WHEN a.qtd_transacoes > 0 
            THEN a.receita_liquida / a.qtd_transacoes 
            ELSE 0 END AS ticket_medio,
            
        CASE WHEN a.quantidade_vendida > 0 
            THEN a.receita_bruta / a.quantidade_vendida 
            ELSE 0 END AS preco_medio_unitario,
            
        CASE WHEN a.quantidade_vendida > 0 
            THEN a.custo_total_ajustado / a.quantidade_vendida 
            ELSE 0 END AS custo_medio_unitario,
        
        -- Comparativo mês anterior
        ant.receita_liquida_ma,
        ant.margem_contribuicao_ma,
        ant.qtd_transacoes_ma,
        
        CASE WHEN ant.receita_liquida_ma > 0 
            THEN ((a.receita_liquida - ant.receita_liquida_ma) / ant.receita_liquida_ma) * 100 
            ELSE 0 END AS var_receita_mom_pct,
            
        CASE WHEN ant.margem_contribuicao_ma > 0 
            THEN ((a.margem_contribuicao - ant.margem_contribuicao_ma) / ant.margem_contribuicao_ma) * 100 
            ELSE 0 END AS var_margem_mom_pct,
        
        CASE WHEN ant.qtd_transacoes_ma > 0 AND a.qtd_transacoes > 0
            THEN (((a.receita_liquida / a.qtd_transacoes) - (ant.receita_liquida_ma / ant.qtd_transacoes_ma)) / 
                  (ant.receita_liquida_ma / ant.qtd_transacoes_ma)) * 100
            ELSE 0 END AS var_ticket_medio_mom_pct,
        
        -- Variação em pontos percentuais da margem
        CASE WHEN ant.receita_liquida_ma > 0 
            THEN (a.margem_contribuicao / a.receita_liquida * 100) - 
                 (ant.margem_contribuicao_ma / ant.receita_liquida_ma * 100)
            ELSE 0 END AS var_margem_pp
        
    FROM metricas_atual a
    CROSS JOIN metricas_anterior ant
)

SELECT * FROM resultado_final