SELECT 
    CODCONTA,
    CONTA,
    GRUPOCONTA,
    CODCONTAMASTER,
    TIPO
FROM
    {{ source('cedep', 'pcconta')}} 