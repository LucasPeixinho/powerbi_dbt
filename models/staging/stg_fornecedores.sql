SELECT
    CODFORNEC,
    FORNECEDOR,
    REVENDA,
    TIPOFORNEC
FROM
    {{ source('cedep', 'pcfornec')}} 