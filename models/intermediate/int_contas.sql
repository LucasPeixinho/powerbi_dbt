SELECT 
    C.CODCONTA,
    C.CONTA,
    C.GRUPOCONTA,
    C.CODCONTAMASTER,
    G.GRUPO
FROM
    {{ ref('stg_contas') }} C
LEFT JOIN 
    {{ ref('stg_grupoconta') }} G
ON 
    C.GRUPOCONTA = G.CODGRUPO