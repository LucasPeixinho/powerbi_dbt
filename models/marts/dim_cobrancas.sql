SELECT
    CODCOB,
    COBRANCA
FROM
    {{ ref('int_cobrancas') }}