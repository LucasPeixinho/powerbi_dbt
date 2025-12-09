SELECT
    CODFORNEC,
    FORNECEDOR,
    REVENDA
FROM
    {{ source('cedep', 'pcfornec')}} 