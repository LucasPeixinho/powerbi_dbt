SELECT 
    CODFILIAL,
    CODCLI,
    CODCOB,
    CODUSUR,
    PREST,
    DUPLIC,
    DTVENC,      -- ✅ Mantém como DATE
    DTPAG,       -- ✅ Mantém como DATE  
    DTEMISSAO,   -- ✅ Mantém como DATE
    VALOR,
    VPAGO
FROM
    cedep.PCPREST
WHERE
    DTVENC BETWEEN TO_DATE('{{ var("dt_inicio", "2018-01-01") }}', 'YYYY-MM-DD')
                AND TO_DATE('{{ var("dt_fim", "2025-12-31") }}', 'YYYY-MM-DD')