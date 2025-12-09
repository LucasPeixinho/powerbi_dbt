SELECT
    CODUSUR,
    NOME_VENDEDOR,
    CODSUPERVISOR
FROM 
    {{ref ('int_vendedores') }}
