with contas as (
    select * from {{ ref('int_contas') }}
),

classificao_contas as (
    select * from {{ ref('seed_classificacao_contas') }}
),

final as (
    select
        c.id_conta,
        c.nome_conta,
        c.id_grupo_conta,
        case
            when c.id_grupo_conta like '1%' then '100'
            when c.id_grupo_conta like '2%' then '200'
            when c.id_grupo_conta like '3%' then '300'
            when c.id_grupo_conta like '4%' then '400'
            when c.id_grupo_conta like '5%' then '500'
            when c.id_grupo_conta like '6%' then '600'
            when c.id_grupo_conta like '7%' then '700'
            when c.id_grupo_conta like '8%' then '800'
            when c.id_grupo_conta like '9%' then '900'
            else null
        end as id_conta_master,
        sc.classificacao
    from
        contas c
    left join
        classificao_contas sc
    on
        sc.codconta = c.id_conta
)

select * from final
