with fornecedores as (
    select * from {{ ref('stg_funcionarios') }}
),

setores as (
    select * from {{ ref('stg_setor') }}
),

renamed as (
    select
        f.id_funcionario,
        f.nome_funcionario,
        s.nome_setor
    from
        stg_funcionarios f
    left join
        stg_setor s
    on
        f.id_setor = s.id_setor
    where
        f.status = 'A'
)

select * from renamed