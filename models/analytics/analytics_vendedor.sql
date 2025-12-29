{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'vendedor']
) }}

WITH vendas AS (
    SELECT * FROM {{ ref('fat_vendas') }}
),

vendedores AS (
    SELECT * FROM {{ ref('dim_vendedores') }}
),

supervisores AS (
    SELECT * FROM {{ ref('dim_supervisores') }}
),

agregacao_vendedor AS (
    SELECT
        v.id_vendedor,
        
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
        COUNT(DISTINCT v.id_fornecedor) AS qtd_fornecedores,
        COUNT(DISTINCT v.id_produto) AS qtd_produtos,
        
        -- Temporalidade
        MIN(v.data_movimento) AS primeira_venda,
        MAX(v.data_movimento) AS ultima_venda
        
    FROM vendas v
    GROUP BY v.id_vendedor
),

metricas_calculadas AS (
    SELECT
        a.*,
        
        -- Dados do vendedor (campos reais da dim_vendedores)
        vd.nome_vendedor,
        vd.id_supervisor,
        vd.vendedor,  -- Campo correto (não status_vendedor)
        
        -- Dados do supervisor
        s.nome_supervisor,
        
        -- Percentuais
        CASE WHEN a.receita_liquida > 0 
            THEN (a.margem_contribuicao / a.receita_liquida) * 100 
            ELSE 0 END AS margem_percentual,
            
        CASE WHEN a.custo_total_ajustado > 0 
            THEN (a.margem_contribuicao / a.custo_total_ajustado) * 100 
            ELSE 0 END AS markup_percentual,
        
        -- Médias
        CASE WHEN a.qtd_transacoes > 0 
            THEN a.receita_liquida / a.qtd_transacoes 
            ELSE 0 END AS ticket_medio,
            
        CASE WHEN a.qtd_clientes > 0 
            THEN a.receita_liquida / a.qtd_clientes 
            ELSE 0 END AS receita_por_cliente,
            
        CASE WHEN a.qtd_clientes > 0 
            THEN CAST(a.qtd_transacoes AS NUMBER) / a.qtd_clientes 
            ELSE 0 END AS transacoes_por_cliente,
        
        -- Participação
        (a.receita_liquida / SUM(a.receita_liquida) OVER ()) * 100 AS participacao_receita_pct,
        
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY a.receita_liquida DESC) AS ranking_receita,
        ROW_NUMBER() OVER (ORDER BY a.margem_contribuicao DESC) AS ranking_margem,
        ROW_NUMBER() OVER (
            PARTITION BY vd.id_supervisor 
            ORDER BY a.receita_liquida DESC
        ) AS ranking_no_supervisor,
        
        -- Status atividade (baseado na última venda)
        CASE 
            WHEN a.ultima_venda >= SYSDATE - 7 THEN 'Muito Ativo'
            WHEN a.ultima_venda >= SYSDATE - 30 THEN 'Ativo'
            WHEN a.ultima_venda >= SYSDATE - 90 THEN 'Inativo Recente'
            ELSE 'Inativo'
        END AS status_atividade
        
    FROM agregacao_vendedor a
    LEFT JOIN vendedores vd ON a.id_vendedor = vd.id_vendedor
    LEFT JOIN supervisores s ON vd.id_supervisor = s.id_supervisor
)

SELECT 
    SYSTIMESTAMP AS data_atualizacao,
    m.*
FROM metricas_calculadas m
ORDER BY receita_liquida DESC