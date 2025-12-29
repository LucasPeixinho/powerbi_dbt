{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'categoria']
) }}

WITH vendas AS (
    SELECT * FROM {{ ref('fat_vendas') }}
),

produtos AS (
    SELECT * FROM {{ ref('dim_produtos') }}
),

agregacao_hierarquia AS (
    SELECT
        p.id_departamento,
        p.nome_departamento,
        p.id_secao,
        p.nome_secao,
        p.id_marca,
        p.nome_marca,
        
        -- Receitas
        SUM(v.receita_bruta) AS receita_bruta,
        SUM(v.receita_liquida) AS receita_liquida,
        SUM(v.valor_desconto) AS valor_desconto,
        
        -- Custos
        SUM(v.custo_total_ajustado) AS custo_total_ajustado,
        
        -- Margem
        SUM(v.margem_contribuicao) AS margem_contribuicao,
        
        -- Volumes
        SUM(v.quantidade) AS quantidade_vendida,
        COUNT(DISTINCT v.id_transacao_venda) AS qtd_transacoes,
        COUNT(DISTINCT v.id_produto) AS qtd_produtos,
        COUNT(DISTINCT v.id_fornecedor) AS qtd_fornecedores,
        COUNT(DISTINCT v.id_cliente) AS qtd_clientes
        
    FROM vendas v
    INNER JOIN produtos p ON v.id_produto = p.id_produto
    GROUP BY 
        p.id_departamento,
        p.nome_departamento,
        p.id_secao,
        p.nome_secao,
        p.id_marca,
        p.nome_marca
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
        
        -- Participação
        (receita_liquida / SUM(receita_liquida) OVER ()) * 100 AS participacao_receita_pct,
        
        -- Médias
        CASE WHEN qtd_transacoes > 0 
            THEN receita_liquida / qtd_transacoes 
            ELSE 0 END AS ticket_medio,
            
        CASE WHEN qtd_produtos > 0 
            THEN receita_liquida / qtd_produtos 
            ELSE 0 END AS receita_por_produto,
        
        -- Rankings por departamento
        ROW_NUMBER() OVER (
            PARTITION BY id_departamento 
            ORDER BY receita_liquida DESC
        ) AS ranking_secao_no_depto,
        
        -- Ranking global
        ROW_NUMBER() OVER (ORDER BY receita_liquida DESC) AS ranking_global
        
    FROM agregacao_hierarquia a
)

SELECT 
    SYSTIMESTAMP AS data_atualizacao,
    m.*
FROM metricas_calculadas m
ORDER BY receita_liquida DESC