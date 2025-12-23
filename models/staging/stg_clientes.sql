with source as (
  select * from {{ source('cedep', 'pcclient') }}
),

final as (
  select
    CODCLI as id_cliente,
    CLIENTE as nome_cliente,
    DTPRIMCOMPRA as data_primeira_compra,
    INICIOATIV as data_inicio_atividade,
    BAIRROCOM as info_extra,
    TIPOFJ as tipo_cliente
  from 
    source
)

select * from final