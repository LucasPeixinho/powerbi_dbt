
with source as (
    select * from {{ source('cedep', 'pcsuperv') }}
),

renamed as (
    select
        CODSUPERVISOR as id_supervisor,
        NOME as nome_supervisor
    from source
)

select * from renamed