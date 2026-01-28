with source as (
    select * from {{ source('cedep', 'pcmovcr') }} 
),

renamed as(
    select
        NUMTRANS as id_transacao,
        HISTORICO as historico,
        DATA as data_transacao,
        DTCONCIL as data_conciliacao,
        TIPO as tipo_transacao,
        VALOR as valor,
        CODFUNCCONCIL as id_funcionario
    from source
)

select * from renamed