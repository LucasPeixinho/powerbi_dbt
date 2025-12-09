SELECT
    CODSUPERVISOR,
    NOME_SUPERVISOR
FROM
    {{ ref('int_supervisores') }}