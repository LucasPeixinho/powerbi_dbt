SELECT
    CODFORNEC,
    FORNECEDOR,
    REVENDA,
    TIPOFORNEC
FROM
    {{ ref('int_fornecedores') }}