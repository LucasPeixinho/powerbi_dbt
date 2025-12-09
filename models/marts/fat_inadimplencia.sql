SELECT 
    CODFILIAL,
    CODCLI,
    CODCOB,
    CODUSUR,
    DTVENC,      
    DTPAG,        
    DTEMISSAO,   
    VALOR,
    VPAGO,
    -- Coluna SITUACAO baseada na lógica DAX
    CASE
        -- PAGO: qualquer pagamento até hoje
        WHEN DTPAG IS NOT NULL AND DTPAG <= TRUNC(SYSDATE) THEN 'PAGO'
        
        -- EM DIA: não está pago, mas vence hoje ou no futuro
        WHEN DTVENC >= TRUNC(SYSDATE) THEN 'EM DIA'
        
        -- ATRASO: vencido, mas dentro da janela de 10 dias
        WHEN DTVENC < TRUNC(SYSDATE) AND DTVENC > TRUNC(SYSDATE) - 10 THEN 'ATRASO'
        
        -- INADIMPLENTE: vencido entre 10 dias e 6 meses
        WHEN DTVENC <= TRUNC(SYSDATE) - 10 AND DTVENC >= ADD_MONTHS(TRUNC(SYSDATE) - 10, -6) THEN 'INADIMPLENTE'
        
        -- PERDIDO: mais de 6 meses atrás
        WHEN DTVENC < ADD_MONTHS(TRUNC(SYSDATE) - 10, -6) THEN 'PERDIDO'
        
        ELSE 'INDEFINIDO'
    END AS SITUACAO

FROM {{ ref('int_contasareceber') }}

WHERE CODCOB NOT IN ('DEVP', 'DEVT', 'BNF', 'BNFT', 'BNFR', 'BNTR', 'BNRP', 'CRED', 'DESD')
  AND {{ filtro_periodo('DTVENC') }}