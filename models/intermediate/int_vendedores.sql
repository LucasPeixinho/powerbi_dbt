SELECT
    CODUSUR,
    NOME,
    CODSUPERVISOR,
    CODUSUR || ' - ' || NOME AS NOME_VENDEDOR
FROM
    {{ ref('stg_vendedores')}} 