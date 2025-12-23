with rateio_cc as (
    select * from {{ ref('stg_rateio_centro_custo') }}
),

final as (
    select
        id_lancamento,
        id_centro_custo,
        percentual_rateio,
        valor
    from rateio_cc
)

select * from final