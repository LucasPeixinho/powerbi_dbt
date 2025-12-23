with clientes as (
    select * from {{ ref('stg_clientes') }}
),

final as (
    select
        id_cliente,
        nome_cliente,
        case
            when data_primeira_compra is null then to_date('01/01/1900', 'DD/MM/YYYY')
            when data_primeira_compra < to_date('01/01/1900', 'DD/MM/YYYY') then to_date('01/01/1900', 'DD/MM/YYYY')
            when data_primeira_compra > sysdate then to_date('01/01/1900', 'DD/MM/YYYY')
            else data_primeira_compra
        end as data_primeira_compra,
        case 
            when data_inicio_atividade is null then to_date('01/01/1900', 'DD/MM/YYYY')
            when data_inicio_atividade < to_date('01/01/1900', 'DD/MM/YYYY') then to_date('01/01/1900', 'DD/MM/YYYY')
            when data_inicio_atividade > sysdate then to_date('01/01/1900', 'DD/MM/YYYY')
            ELSE data_inicio_atividade
        end as data_inicio_atividade,
        info_extra,
        case
            when 
                tipo_cliente = 'F' 
            then 'Física' 
            else 'Jurídica' 
        end as tipo_cliente
    from 
        clientes
)

select * from final

