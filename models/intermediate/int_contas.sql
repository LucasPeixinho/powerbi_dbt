with contas as (
    select * from {{ ref('stg_contas') }}
),

grupo_conta as(
    select * from {{ ref('stg_grupoconta')}}
),

final as (
    select 
        c.id_conta,
        c.nome_conta,
        c.id_grupo_conta,
        c.id_conta_master,
        g.nome_grupo_conta
    from
        contas c
    left join
        grupo_conta g
    on
        c.id_grupo_conta = g.id_grupo_conta
)

select * from final