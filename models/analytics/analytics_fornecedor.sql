{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'fornecedor']
) }}

WITH vendas AS (
    SELECT * FROM {{ ref('fat_vendas') }}
),

fornecedores AS (
    SELECT * FROM {{ ref('dim_fornecedores') }}
),

departamento AS (
    SELECT * FROM {{ ref('dim_departamento') }}
),

agregacao_fornecedor AS (
    SELECT
        v.id_fornecedor,
        v.id_departamento,
        EXTRACT(YEAR FROM v.data_movimento) as ano,
        
        -- Receitas
        SUM(v.receita_bruta) AS receita_bruta,
        SUM(v.receita_liquida) AS receita_liquida,
        SUM(v.valor_desconto) AS valor_desconto,
        
        -- Custos
        SUM(v.custo_total) AS custo_produto,
        SUM(v.valor_frete_rateado) AS custo_frete,
        SUM(v.valor_outras_despesas_rateado) AS custo_outras_despesas,
        SUM(v.custo_total_ajustado) AS custo_total_ajustado,
        SUM(v.valor_st) AS valor_st,
        SUM(v.valor_ipi) AS valor_ipi,
        
        -- Margem
        SUM(v.margem_contribuicao) AS margem_contribuicao,
        
        -- Volumes
        SUM(v.quantidade) AS quantidade_vendida,
        COUNT(DISTINCT v.id_transacao_venda) AS qtd_transacoes,
        COUNT(DISTINCT v.id_produto) AS qtd_produtos,
        COUNT(DISTINCT v.id_cliente) AS qtd_clientes,
        COUNT(DISTINCT v.id_vendedor) AS qtd_vendedores,
        COUNT(DISTINCT v.id_departamento) AS qtd_departamentos,
        
        -- Primeira e última venda
        MIN(v.data_movimento) AS primeira_venda,
        MAX(v.data_movimento) AS ultima_venda
        
    FROM vendas v
    GROUP BY v.id_fornecedor, EXTRACT(YEAR FROM v.data_movimento), v.id_departamento
),

metricas_calculadas AS (
    SELECT
        a.*,
        
        -- Dados do fornecedor
        d.nome_departamento,
        f.nome_fornecedor,
        f.finalidade_fornecedor,
        f.tipo_fornecedor,
        f.uf_fornecedor,
        f.cidade_fornecedor,
        f.status_bloqueio,
        
        -- Percentuais
        CASE WHEN a.receita_liquida > 0 
            THEN (a.margem_contribuicao / a.receita_liquida) * 100 
            ELSE 0 END AS margem_percentual,
            
        CASE WHEN a.custo_total_ajustado > 0 
            THEN (a.margem_contribuicao / a.custo_total_ajustado) * 100 
            ELSE 0 END AS markup_percentual,
            
        CASE WHEN a.receita_bruta > 0 
            THEN (a.valor_desconto / a.receita_bruta) * 100 
            ELSE 0 END AS desconto_percentual,
        
        -- Médias
        CASE WHEN a.qtd_transacoes > 0 
            THEN a.receita_liquida / a.qtd_transacoes 
            ELSE 0 END AS ticket_medio,
            
        CASE WHEN a.qtd_produtos > 0 
            THEN a.receita_liquida / a.qtd_produtos 
            ELSE 0 END AS receita_por_produto,
            
        CASE WHEN a.quantidade_vendida > 0 
            THEN a.receita_liquida / a.quantidade_vendida 
            ELSE 0 END AS preco_medio_unitario,
        
        -- Participação sobre total (usando window functions)
        (a.receita_liquida / SUM(a.receita_liquida) OVER ()) * 100 AS participacao_receita_pct,
        (a.margem_contribuicao / SUM(a.margem_contribuicao) OVER ()) * 100 AS participacao_margem_pct,
        (a.quantidade_vendida / SUM(a.quantidade_vendida) OVER ()) * 100 AS participacao_quantidade_pct,
        
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY a.receita_liquida DESC) AS ranking_receita,
        ROW_NUMBER() OVER (ORDER BY a.margem_contribuicao DESC) AS ranking_margem,
        ROW_NUMBER() OVER (ORDER BY (a.margem_contribuicao / NULLIF(a.receita_liquida, 0)) DESC) AS ranking_margem_pct
        
    FROM agregacao_fornecedor a
    LEFT JOIN fornecedores f ON a.id_fornecedor = f.id_fornecedor
    LEFT JOIN departamento d ON a.id_departamento = d.id_departamento
),

curva_abc AS (
    SELECT
        m.*,
        
        -- Receita acumulada para curva ABC
        SUM(receita_liquida) OVER (
            ORDER BY receita_liquida DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS receita_acumulada,
        
        (SUM(receita_liquida) OVER (
            ORDER BY receita_liquida DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / SUM(receita_liquida) OVER ()) * 100 AS percentual_acumulado,
        
        -- Classificação ABC
        CASE 
            WHEN (SUM(receita_liquida) OVER (
                ORDER BY receita_liquida DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / SUM(receita_liquida) OVER ()) <= 0.80 THEN 'A'
            WHEN (SUM(receita_liquida) OVER (
                ORDER BY receita_liquida DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / SUM(receita_liquida) OVER ()) <= 0.95 THEN 'B'
            ELSE 'C'
        END AS curva_abc,
        
        -- Faixa de margem
        CASE 
            WHEN (margem_contribuicao / NULLIF(receita_liquida, 0)) * 100 >= 30 THEN 'Alta (≥30%)'
            WHEN (margem_contribuicao / NULLIF(receita_liquida, 0)) * 100 >= 15 THEN 'Média (15-30%)'
            WHEN (margem_contribuicao / NULLIF(receita_liquida, 0)) * 100 >= 0 THEN 'Baixa (<15%)'
            ELSE 'Negativa'
        END AS faixa_margem,
        
        -- Status atividade
        CASE 
            WHEN ultima_venda >= SYSDATE - 30 THEN 'Ativo'
            WHEN ultima_venda >= SYSDATE - 90 THEN 'Inativo Recente'
            ELSE 'Inativo'
        END AS status_atividade
        
    FROM metricas_calculadas m
)

SELECT 
    SYSTIMESTAMP AS data_atualizacao,
    c.*
FROM curva_abc c
ORDER BY receita_liquida DESC