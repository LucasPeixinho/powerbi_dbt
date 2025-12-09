SELECT
    CODSUPERVISOR,
    NOME,
    CODSUPERVISOR || ' - ' || NOME as NOME_SUPERVISOR
FROM
    {{ ref('stg_supervisores')}} 