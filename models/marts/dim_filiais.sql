SELECT
    codigo,
    razaosocial,
    endereco,
    cidade,
    CASE
        WHEN codigo IN ( '2', '9', '10' ) THEN
            'ATACADO'
        ELSE
            'VAREJO'
    END AS tipo
FROM
    {{ ref('int_filiais')}}