with funcionarios as (
    select * from {{ ref('int_funcionarios') }}
),

final as (
    select
        id_funcionario, 
        nome_funcionario,
        nome_setor
    from
        funcionarios
)

select * from final