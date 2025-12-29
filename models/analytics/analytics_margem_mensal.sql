{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'temporal']
) }}

WITH vendas AS (
    SELECT * FROM {{ ref('fat_vendas') }}
),

agregacao_mensal AS (
    SELECT
        TRUNC(data_movimento, 'MONTH') AS ano_mes,
        EXTRACT(YEAR FROM data_movimento) AS ano,
        EXTRACT(MONTH FROM data_movimento) AS mes,
        TO_CHAR(data_movimento, 'YYYY-MM') AS ano_mes_str,
        TO_CHAR(data_movimento, 'Mon/YYYY', 'NLS_DATE_LANGUAGE=PORTUGUESE') AS mes_ano_nome,
        
        -- Receitas
        SUM(receita_bruta) AS receita_bruta,
        SUM(receita_liquida) AS receita_liquida,
        SUM(valor_desconto) AS valor_desconto,
        
        -- Custos
        SUM(custo_total) AS custo_produto,
        SUM(valor_frete_rateado) AS custo_frete,
        SUM(valor_outras_despesas_rateado) AS custo_outras_despesas,
        SUM(custo_total_ajustado) AS custo_total_ajustado,
        SUM(valor_st) AS valor_st,
        SUM(valor_ipi) AS valor_ipi,
        
        -- Margem
        SUM(margem_contribuicao) AS margem_contribuicao,
        
        -- Volumes
        SUM(quantidade) AS quantidade_vendida,
        COUNT(DISTINCT id_transacao_venda) AS qtd_transacoes,
        COUNT(DISTINCT id_fornecedor) AS qtd_fornecedores,
        COUNT(DISTINCT id_produto) AS qtd_produtos,
        COUNT(DISTINCT id_cliente) AS qtd_clientes,
        COUNT(DISTINCT id_vendedor) AS qtd_vendedores
        
    FROM vendas
    GROUP BY 
        TRUNC(data_movimento, 'MONTH'),
        EXTRACT(YEAR FROM data_movimento),
        EXTRACT(MONTH FROM data_movimento),
        TO_CHAR(data_movimento, 'YYYY-MM'),
        TO_CHAR(data_movimento, 'Mon/YYYY', 'NLS_DATE_LANGUAGE=PORTUGUESE')
),

metricas_calculadas AS (
    SELECT
        a.*,
        
        -- Percentuais
        CASE WHEN receita_liquida > 0 
            THEN (margem_contribuicao / receita_liquida) * 100 
            ELSE 0 END AS margem_percentual,
            
        CASE WHEN custo_total_ajustado > 0 
            THEN (margem_contribuicao / custo_total_ajustado) * 100 
            ELSE 0 END AS markup_percentual,
            
        CASE WHEN receita_bruta > 0 
            THEN (valor_desconto / receita_bruta) * 100 
            ELSE 0 END AS desconto_percentual,
        
        -- Médias
        CASE WHEN qtd_transacoes > 0 
            THEN receita_liquida / qtd_transacoes 
            ELSE 0 END AS ticket_medio,
            
        CASE WHEN quantidade_vendida > 0 
            THEN receita_bruta / quantidade_vendida 
            ELSE 0 END AS preco_medio_unitario,
            
        CASE WHEN quantidade_vendida > 0 
            THEN custo_total_ajustado / quantidade_vendida 
            ELSE 0 END AS custo_medio_unitario,
        
        -- Variação MoM (usando LAG)
        LAG(receita_liquida, 1) OVER (ORDER BY ano_mes) AS receita_liquida_mes_anterior,
        LAG(margem_contribuicao, 1) OVER (ORDER BY ano_mes) AS margem_mes_anterior,
        LAG(margem_contribuicao / NULLIF(receita_liquida, 0) * 100, 1) OVER (ORDER BY ano_mes) AS margem_pct_mes_anterior,
        
        CASE 
            WHEN LAG(receita_liquida, 1) OVER (ORDER BY ano_mes) > 0 
            THEN ((receita_liquida - LAG(receita_liquida, 1) OVER (ORDER BY ano_mes)) / 
                  LAG(receita_liquida, 1) OVER (ORDER BY ano_mes)) * 100 
            ELSE 0 
        END AS var_receita_mom_pct,
        
        CASE 
            WHEN LAG(margem_contribuicao, 1) OVER (ORDER BY ano_mes) > 0 
            THEN ((margem_contribuicao - LAG(margem_contribuicao, 1) OVER (ORDER BY ano_mes)) / 
                  LAG(margem_contribuicao, 1) OVER (ORDER BY ano_mes)) * 100 
            ELSE 0 
        END AS var_margem_mom_pct,
        
        -- Acumulado YTD
        SUM(receita_liquida) OVER (
            PARTITION BY ano 
            ORDER BY mes 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS receita_liquida_ytd,
        
        SUM(margem_contribuicao) OVER (
            PARTITION BY ano 
            ORDER BY mes 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS margem_contribuicao_ytd,
        
        CASE 
            WHEN SUM(receita_liquida) OVER (
                PARTITION BY ano 
                ORDER BY mes 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) > 0
            THEN (SUM(margem_contribuicao) OVER (
                PARTITION BY ano 
                ORDER BY mes 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / SUM(receita_liquida) OVER (
                PARTITION BY ano 
                ORDER BY mes 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )) * 100
            ELSE 0
        END AS margem_percentual_ytd
        
    FROM agregacao_mensal a
)

SELECT 
    SYSTIMESTAMP AS data_atualizacao,
    m.*
FROM metricas_calculadas m
ORDER BY ano_mes