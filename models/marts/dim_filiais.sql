with filiais as (
    select * from int_filiais
),

final as (
    select
        id_filial,
        razao_social_filial,
        endereco_filial,
        cidade_filial,
        case when id_filial in ('2', '9', '10')
            then 'ATACADO'
            else 'VAREJO'
        end as tipo_filial
    from filiais
)

select * from final