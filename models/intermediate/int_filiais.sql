with filiais as (
    select * from {{ ref('stg_filiais') }}
),

final as (
    select
        id_filial,
        razao_social_filial,
        endereco_filial,
        cidade_filial
    from 
        filiais
)

select * from final