{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'produto']
) }}

WITH vendas AS (
    SELECT * FROM {{ ref('fat_vendas') }}
),

produtos AS (
    SELECT * FROM {{ ref('dim_produtos') }}
),

agregacao_produto AS (
    SELECT
        v.id_produto,
        v.id_fornecedor,
        v.id_departamento,
        v.id_secao,
        v.id_marca,
        
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
        COUNT(DISTINCT v.id_cliente) AS qtd_clientes,
        
        -- Valores unitários médios
        AVG(v.preco_unitario) AS preco_unitario_medio,
        AVG(v.custo_unitario) AS custo_unitario_medio,
        
        -- Temporalidade
        MIN(v.data_movimento) AS primeira_venda,
        MAX(v.data_movimento) AS ultima_venda
        
    FROM vendas v
    GROUP BY 
        v.id_produto, 
        v.id_fornecedor, 
        v.id_departamento, 
        v.id_secao, 
        v.id_marca
),

metricas_calculadas AS (
    SELECT
        a.*,
        
        -- Atributos do produto
        p.descricao_produto,
        p.nome_departamento,
        p.nome_secao,
        p.nome_marca,
        p.custo_reposicao,
        p.quantidade_por_caixa,
        p.unidade_medida,
        
        -- Percentuais
        CASE WHEN a.receita_liquida > 0 
            THEN (a.margem_contribuicao / a.receita_liquida) * 100 
            ELSE 0 END AS margem_percentual,
            
        CASE WHEN a.custo_total_ajustado > 0 
            THEN (a.margem_contribuicao / a.custo_total_ajustado) * 100 
            ELSE 0 END AS markup_percentual,
        
        -- Participação
        (a.receita_liquida / SUM(a.receita_liquida) OVER ()) * 100 AS participacao_receita_pct,
        
        -- Ranking global
        ROW_NUMBER() OVER (ORDER BY a.receita_liquida DESC) AS ranking_receita_global,
        
        -- Ranking dentro do fornecedor
        ROW_NUMBER() OVER (
            PARTITION BY a.id_fornecedor 
            ORDER BY a.receita_liquida DESC
        ) AS ranking_receita_fornecedor,
        
        -- Status
        CASE 
            WHEN a.ultima_venda >= SYSDATE - 30 THEN 'Ativo'
            WHEN a.ultima_venda >= SYSDATE - 90 THEN 'Inativo Recente'
            ELSE 'Inativo'
        END AS status_produto
        
    FROM agregacao_produto a
    LEFT JOIN produtos p ON a.id_produto = p.id_produto
)

SELECT 
    SYSTIMESTAMP AS data_atualizacao,
    m.*
FROM metricas_calculadas m
ORDER BY receita_liquida DESC