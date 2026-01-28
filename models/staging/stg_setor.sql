with source as(
    select * from {{ source('cedep', 'pcsetor') }} 
),

renamed as(
    select
        CODSETOR as id_setor,
        DESCRICAO as nome_setor
    from source
)

select * from renamed