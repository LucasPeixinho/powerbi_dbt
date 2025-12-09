SELECT
    CODFORNEC,
    FORNECEDOR,
    CASE REVENDA
        WHEN 'T' THEN 'Transportadora'
        WHEN 'B' THEN 'Beneficiamento'
        WHEN 'C' THEN 'Comunicação'
        WHEN 'O' THEN 'Consumo'
        WHEN 'S' THEN 'É Revenda'
        WHEN 'E' THEN 'Energia'
        WHEN 'X' THEN 'Exportador'
        WHEN 'N' THEN 'Não Revenda'
        WHEN 'P' THEN 'Profissional liberal'
        ELSE 'Desconhecido'
    END AS TIPO_FORNECEDOR
FROM
    {{ ref('stg_fornecedores') }} 