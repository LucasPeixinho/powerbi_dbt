with source as (
    select * from {{ source('cedep', 'pcmarca') }}
),

renamed as (
    select
        CODMARCA as id_marca,
        MARCA as nome_marca
    from source
)

select * from renamed