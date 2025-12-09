SELECT
    CODCOB,
    COBRANCA
FROM
    {{ ref('stg_cobranca') }}