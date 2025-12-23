with source as (
    select * from {{ source('cedep', 'pcusuari') }}
),

renamed as (
    select
        CODUSUR as id_vendedor,
        NOME as nome_vendedor,
        CODSUPERVISOR as id_supervisor
    from source
)

select * from renamed