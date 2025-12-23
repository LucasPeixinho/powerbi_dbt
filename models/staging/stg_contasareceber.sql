SELECT 
    CODFILIAL,
    CODCLI,
    CODCOB,
    CODCOBORIG,
    CODUSUR,
    PREST,
    DUPLIC,
    DTVENC,      
    DTPAG,        
    DTEMISSAO,   
    VALOR,
    VPAGO
FROM
    {{ source('cedep', 'pcprest')}}
    