SELECT
    codcob,
    cobranca,
    CASE
        WHEN codcob IN ( '001', '004', '237', '341', '422',
                         'B001', 'B033', 'B341', 'B422', 'BK',
                         'C', 'C001', 'C341', 'C707', 'CHD1',
                         'CHD3', 'CHDV', 'CHI', 'CHP', 'CHPC',
                         'CHV' ) THEN
            'A PRAZO'
        ELSE
            'A VISTA'
    END AS tipo
FROM
    {{ ref('int_cobrancas') }}