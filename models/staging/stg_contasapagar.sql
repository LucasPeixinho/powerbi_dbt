SELECT 
    RECNUM,
    NUMNOTA,
    CODCONTA,
    CODFORNEC,
    CODFILIAL,
    DTEMISSAO,
    DTLANC,
    DTVENC,
    DTPAGTO,
    DTESTORNOBAIXA,
    DTCOMPETENCIA,
    DUPLIC,
    VALOR,
    VPAGO,
    VLVARIACAOCAMBIAL,
    TIPOLANC
FROM {{ source('cedep', 'pclanc')}}
--WHERE DTLANC BETWEEN TO_DATE('{{ var("dt_inicio", "2020-01-01") }}', 'YYYY-MM-DD') 
--                AND TO_DATE('{{ var("dt_fim", "2025-12-31") }}', 'YYYY-MM-DD')
-- ✅ Com filtro de DTLANC para o primeiro SELECT do UNION
