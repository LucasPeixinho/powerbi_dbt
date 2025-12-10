SELECT 
    CODGRUPO,
    GRUPO
FROM
    {{ ref('stg_grupoconta') }}