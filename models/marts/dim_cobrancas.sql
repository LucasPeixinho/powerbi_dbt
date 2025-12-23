with cobrancas as (
    select * from {{ ref('int_cobrancas') }}
),

final as (
    select
        id_cobranca,
        nome_cobranca,
        case
            when id_cobranca in ( '001', '004', '237', '341', '422',
                                'B001', 'B033', 'B341', 'B422', 'BK',
                                'C', 'C001', 'C341', 'C707', 'CHD1',
                                'CHD3', 'CHDV', 'CHI', 'CHP', 'CHPC',
                                'CHV' )
            then 'A PRAZO'
            else 'A VISTA'
        end as tipo_cobranca
    from cobrancas
)

select * from final