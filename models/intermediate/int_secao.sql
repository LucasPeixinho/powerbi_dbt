with secao as (
    select * from {{ ref('stg_secao') }}
),

final as (
    select
        id_secao,
        nome_secao,
        id_departamento
    from secao
)

select * from final