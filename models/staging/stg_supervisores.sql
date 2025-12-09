SELECT
    CODSUPERVISOR,
    NOME
FROM
    {{ source('cedep', 'pcsuperv')}} 