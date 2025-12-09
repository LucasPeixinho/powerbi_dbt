SELECT
    CODUSUR,
    NOME,
    CODSUPERVISOR
FROM
    {{ source('cedep', 'pcusuari')}} 