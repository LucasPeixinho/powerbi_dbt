SELECT
    RECNUMADIANTAMENTO,
    RECNUMPAGTO,
    VALOR,
    DTLANC, 
    DTESTORNO
FROM 
    {{ source('cedep', 'pclancadiantfornec')}}
WHERE 
    DTESTORNO IS NULL
/* AND
--    DTLANC
BETWEEN 
    TO_DATE('{{ var("dt_inicio", "2020-01-01") }}', 'YYYY-MM-DD')
AND
    TO_DATE('{{ var("dt_fim", "2025-12-31") }}', 'YYYY-MM-DD') */