with grupo_conta as (
    select * from {{ ref('stg_grupoconta') }}
),

final as (
    select
        id_grupo_conta,
        nome_grupo_conta
    from grupo_conta
)

select * from final