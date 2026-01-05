with source as (
    select * from {{ source('cedep', 'pcbanco') }}
),

renamed as (
    select
        CODBANCO as id_banco,
        NOME as nome_banco
    from source
)

select * from renamed