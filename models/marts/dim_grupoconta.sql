SELECT 
    CODGRUPO,
    GRUPO
FROM
    {{ ref('int_grupoconta') }}