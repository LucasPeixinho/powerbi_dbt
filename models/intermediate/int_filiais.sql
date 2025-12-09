SELECT
    CODIGO,
    RAZAOSOCIAL,
    ENDERECO,
    CIDADE
FROM
    {{ ref('stg_filiais')}} 