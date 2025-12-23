with source as (
    select * from {{ source('cedep','pcgrupo') }}
),

final as (
    select
        CODGRUPO as id_grupo_conta,
        GRUPO as nome_grupo_conta
    from
        source
)

select * from final