SELECT
    CODIGO,
    RAZAOSOCIAL,
    ENDERECO,
    CIDADE
FROM
    {{ ref('int_filiais')}} 