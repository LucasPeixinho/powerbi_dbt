SELECT 
    CODGRUPO,
    GRUPO
FROM
    {{ source('cedep','pcgrupo')}}