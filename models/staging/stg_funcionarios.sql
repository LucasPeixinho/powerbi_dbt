with source as(
    select * from {{ source('cedep', 'pcempr') }} 
),

renamed as(
    select
        MATRICULA as id_funcionario,
        NOME as nome_funcionario,
        CODSETOR as id_setor,
        SITUACAO as status
    from source
)

select * from renamed