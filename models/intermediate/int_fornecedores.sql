SELECT
    CODFORNEC,
    FORNECEDOR,
    CASE REVENDA
        WHEN 'T' THEN 'Transportadora'
        WHEN 'B' THEN 'Beneficiamento'
        WHEN 'C' THEN 'Comunicação'
        WHEN 'O' THEN 'Consumo'
        WHEN 'S' THEN 'Revenda'
        WHEN 'E' THEN 'Energia'
        WHEN 'X' THEN 'Exportador'
        WHEN 'N' THEN 'Não Revenda'
        WHEN 'P' THEN 'Profissional liberal'
        ELSE 'Desconhecido'
    END AS REVENDA,
    CASE TIPOFORNEC
        WHEN 'D' THEN 'Central de distribuição'
        WHEN 'C' THEN 'Comércio atacadista'
        WHEN 'V' THEN 'Varejista'
        WHEN 'F' THEN 'Filial'
        WHEN 'I' THEN 'Indústria'
        WHEN 'O' THEN 'Outras'
    END AS TIPOFORNEC
FROM
    {{ ref('stg_fornecedores') }} 