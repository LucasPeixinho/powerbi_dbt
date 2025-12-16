SELECT 
    BA.RECNUM,
    LA.NUMNOTA,
    LA.CODCONTA,
    LA.CODFORNEC,
    LA.CODFILIAL,
    LA.DTEMISSAO,
    LA.DTLANC,
    LA.DTVENC,
    BA.DTPAGTO,
    LA.DTCOMPETENCIA,
    LA.DUPLIC,
    ((BA.VPAGO + NVL(BA.VLVARIACAOCAMBIAL, 0)) * (-1)) AS VALOR,
    ((BA.VPAGO + NVL(BA.VLVARIACAOCAMBIAL, 0)) * (-1)) AS VPAGO,
    LA.TIPOLANC
FROM {{ ref('stg_lanc_adiant') }} A
JOIN {{ ref('stg_contasapagar') }} LA  -- ✅ USA a staging SEM filtro de data!
    ON A.RECNUMADIANTAMENTO = LA.RECNUM
JOIN {{ ref('stg_contasapagar') }} BA  -- ✅ USA a staging SEM filtro de data!
    ON A.RECNUMPAGTO = BA.RECNUM
WHERE BA.DTPAGTO IS NOT NULL
  AND BA.DTESTORNOBAIXA IS NULL
  AND LA.CODFILIAL IN ('1', '10', '2', '3', '4', '7', '8', '9', '99')
-- ✅ Sem filtro de DTLANC em LA e BA (apenas em A via stg_lanc_adiant)
