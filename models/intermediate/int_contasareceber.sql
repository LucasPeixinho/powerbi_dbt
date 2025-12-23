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
    {{ ref('stg_contasareceber')}}