SELECT 
    RECNUM,
    CODIGOCENTROCUSTO,
    PERCRATEIO,
    VALOR
FROM {{ source('cedep', 'pcrateiocentrocusto') }}
