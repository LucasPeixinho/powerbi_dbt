SELECT
    CODFORNEC,
    FORNECEDOR,
    TIPO_FORNECEDOR
FROM
    {{ ref('int_fornecedores') }}