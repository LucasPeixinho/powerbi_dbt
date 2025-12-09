WITH lancamentos_base AS (
    -- Primeira parte: lançamentos diretos (com filtro de DTLANC, sem CODFILIAL)
    SELECT 
        RECNUM,
        CODCONTA,
        CODFORNEC,
        CODFILIAL,
        DTEMISSAO,
        DTLANC,
        DTVENC,
        DTPAGTO,
        DTCOMPETENCIA,
        VALOR,
        VPAGO
    FROM {{ ref('stg_contasapagar') }}  -- ✅ Usa a staging COM filtro de data
    
    UNION ALL
    
    -- Segunda parte: baixas de adiantamento (filtros aplicados no int_lanc_adiant)
    SELECT 
        RECNUM,
        CODCONTA,
        CODFORNEC,
        CODFILIAL,
        DTEMISSAO,
        DTLANC,
        DTVENC,
        DTPAGTO,
        DTCOMPETENCIA,
        VALOR,
        VPAGO
    FROM {{ ref('int_lanc_adiant') }}  -- ✅ Já tem todos os filtros corretos
)
SELECT * FROM lancamentos_base