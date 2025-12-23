with supervisores as (
    select * from {{ ref('stg_supervisores') }}
),

final as (
    select
        id_supervisor,
        nome_supervisor,
        id_supervisor || ' - ' || nome_supervisor as supervisor
    from supervisores
)

select * from final