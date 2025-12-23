with source as (
    select * from {{ source('cedep', 'pccob') }}
),

final as (
    select 
        CODCOB as id_cobranca,
        COBRANCA as nome_cobranca
    from 
        source
)

select * from final