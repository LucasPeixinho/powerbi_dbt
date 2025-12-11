SELECT
    RECNUM,
    CODFORNEC,
    DTLANC,
    DTVENC,
    VALOR,
    TIPOLANC
FROM
    {{ ref('int_lanc_futuro') }}
