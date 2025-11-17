SELECT 
    CODFILIAL,
    CODCLI,
    CODCOB,
    CODUSUR,
    PREST,
    DUPLIC,
    TO_CHAR(DTVENC, 'DD-MM-YYYY') as DTVENC,
    TO_CHAR(DTPAG, 'DD-MM-YYYY') as DTPAG,
    TO_CHAR(DTEMISSAO, 'DD-MM-YYYY') as DTEMISSAO,
    VALOR,
    VPAGO
FROM
    cedep.PCPREST
WHERE
    DTVENC BETWEEN TO_DATE('{{ var("dt_inicio", "2018-01-01") }}', 'YYYY-MM-DD')
                AND TO_DATE('{{ var("dt_fim", "2025-12-31") }}', 'YYYY-MM-DD')