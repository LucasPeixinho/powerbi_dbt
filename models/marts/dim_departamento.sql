with departamentos as (
    select * from {{ ref('int_departamento') }}
),

final as (
    select
        id_departamento,
        nome_departamento
    from
        departamentos
)

select * from final