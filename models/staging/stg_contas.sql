with source as (
    select * from {{ source('cedep', 'pcconta') }}
),

final as (
    select
        CODCONTA as id_conta, 
        CONTA as nome_conta, 
        GRUPOCONTA as id_grupo_conta, 
        CODCONTAMASTER as id_conta_master,
        TIPO as tipo_conta
    from 
        source
)

select * from final