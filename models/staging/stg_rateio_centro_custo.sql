with source as (
    select * from {{ source('cedep', 'pcrateiocentrocusto') }}
),

final as (
    select
        RECNUM as id_lancamento, 
        CODIGOCENTROCUSTO as id_centro_custo,
        PERCRATEIO as percentual_rateio, 
        VALOR as valor
    from
        source
)

select * from final