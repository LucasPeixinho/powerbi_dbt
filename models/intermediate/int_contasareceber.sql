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
    {{ ref('stg_contasareceber')}}