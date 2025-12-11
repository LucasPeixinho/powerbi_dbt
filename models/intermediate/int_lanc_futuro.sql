SELECT
    RECNUM,
    CODFORNEC,
    DTLANC,
    DTVENC,
    VALOR,
    CASE
        WHEN TIPOLANC = 'C' THEN 'CONFIRMADO'
        WHEN TIPOLANC = 'P' THEN 'PROVISIONADO'
        ELSE 'NÃO INFORMADO'
    END AS TIPOLANC
FROM
    {{ ref('stg_lanc_futuros') }}