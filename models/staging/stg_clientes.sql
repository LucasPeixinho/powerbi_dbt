SELECT
    CODCLI,
    CLIENTE,
    DTPRIMCOMPRA,
    INICIOATIV,
    BAIRROCOM,
    TIPOFJ

FROM {{ source('cedep', 'pcclient')}} 
