{{ config(
    materialized='table',
    tags=['analytics', 'streamlit', 'estrategia']
) }}

WITH ranking_fornecedores AS (
    SELECT * FROM {{ ref('analytics_fornecedor') }}
),

estatisticas AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY receita_liquida) AS mediana_receita,
        AVG(margem_percentual) AS media_margem_pct,
        25.0 AS meta_margem_pct
    FROM ranking_fornecedores
    WHERE ano >= EXTRACT(YEAR FROM SYSDATE)
      AND ano <= EXTRACT(YEAR FROM SYSDATE)
),

classificacao AS (
    SELECT
        r.*,
        e.mediana_receita,
        e.media_margem_pct,
        e.meta_margem_pct,
        
        -- Quadrante estratégico
        CASE 
            WHEN r.receita_liquida >= e.mediana_receita AND r.margem_percentual >= e.meta_margem_pct 
                THEN '⭐ Estrela'
            WHEN r.receita_liquida < e.mediana_receita AND r.margem_percentual >= e.meta_margem_pct 
                THEN '💎 Nicho'
            WHEN r.receita_liquida >= e.mediana_receita AND r.margem_percentual < e.meta_margem_pct 
                THEN '⚠️ Negociar'
            ELSE '🔴 Cortar'
        END AS quadrante_estrategico,
        
        -- Score de atratividade (0-100)
        ((LEAST(r.margem_percentual / 50.0, 1.0) * 0.6) + 
         (LEAST(r.participacao_receita_pct / 10.0, 1.0) * 0.4)) * 100 AS score_atratividade,
        
        -- Distância da meta de margem
        r.margem_percentual - e.meta_margem_pct AS gap_meta_margem_pp,
        
        -- Classificação de prioridade
        CASE 
            WHEN r.receita_liquida >= e.mediana_receita AND r.margem_percentual < e.meta_margem_pct 
                THEN 'ALTA - Revisar Preços'
            WHEN r.receita_liquida >= e.mediana_receita AND r.margem_percentual >= e.meta_margem_pct 
                THEN 'ALTA - Manter/Expandir'
            WHEN r.receita_liquida < e.mediana_receita AND r.margem_percentual >= e.meta_margem_pct 
                THEN 'MÉDIA - Avaliar Potencial'
            ELSE 'BAIXA - Avaliar Descontinuidade'
        END AS prioridade_gestao
        
    FROM ranking_fornecedores r
    CROSS JOIN estatisticas e
)

SELECT 
    SYSTIMESTAMP AS data_processamento,
    c.*
FROM classificacao c
ORDER BY score_atratividade DESC