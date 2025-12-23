with source as (
    select * from {{ source('cedep', 'pcsecao') }}
),

renamed as (
    select
        CODSEC as id_secao,
        DESCRICAO as nome_secao,
        CODEPTO as id_departamento
    from source
)

select * from renamed