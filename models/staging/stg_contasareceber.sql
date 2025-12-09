SELECT 
    CODFILIAL,
    CODCLI,
    CODCOB,
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
    