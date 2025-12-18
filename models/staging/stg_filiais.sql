SELECT
    codigo,
    razaosocial,
    endereco,
    cidade
FROM
    {{ source('cedep', 'pcfilial')}}
WHERE
    codigo <> '99'